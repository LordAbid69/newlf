// scraper.js — final version
'use strict';

// ── Find bundled Chromium ─────────────────────────────────────────────────────
function findChromiumExecutable() {
  var fs   = require('fs');
  var path = require('path');

  var browsersRoot = process.env.PLAYWRIGHT_BROWSERS_PATH;
  if (!browsersRoot) {
    browsersRoot = process.resourcesPath
      ? path.join(process.resourcesPath, 'browsers')
      : path.join(__dirname, 'browsers');
  }

  if (!fs.existsSync(browsersRoot)) {
    throw new Error('Browsers folder not found: ' + browsersRoot + '\nRun BUILD.bat first.');
  }

  function walk(dir) {
    var items = fs.readdirSync(dir);
    for (var i = 0; i < items.length; i++) {
      var full = path.join(dir, items[i]);
      if (items[i].toLowerCase() === 'chrome.exe') return full;
      try {
        if (fs.statSync(full).isDirectory()) {
          var found = walk(full);
          if (found) return found;
        }
      } catch (_) {}
    }
    return null;
  }

  var exe = walk(browsersRoot);
  if (!exe) {
    var tree = '';
    try {
      fs.readdirSync(browsersRoot).forEach(function(e) {
        tree += e + '\n';
        try { fs.readdirSync(path.join(browsersRoot, e)).forEach(function(f) { tree += '    ' + f + '\n'; }); } catch (_) {}
      });
    } catch (_) {}
    throw new Error('chrome.exe not found in: ' + browsersRoot + '\nContents:\n' + tree);
  }
  return exe;
}

// ── Main export ───────────────────────────────────────────────────────────────
module.exports = async function runScraper(config, callbacks, stopSignal) {
  const { chromium } = require('playwright-core');
  const xlsx = require('xlsx');
  const fs   = require('fs');
  const path = require('path');

  const LISTING_URL = config.url;
  const MAX_PAGES   = config.maxPages;
  const CONCURRENCY = config.concurrency;
  const saveFolder  = config.saveFolder;
  const fileName    = config.fileName;

  const { onLog, onRow, onProgress, onDone } = callbacks;
  const log   = (level, text) => onLog(level, text);
  const sleep = (ms) => new Promise((res) => setTimeout(res, ms));

  let BASE;
  try { const u = new URL(LISTING_URL); BASE = u.protocol + '//' + u.host; }
  catch (_) { BASE = 'https://www.marktstammdatenregister.de'; }

  if (!fs.existsSync(saveFolder)) fs.mkdirSync(saveFolder, { recursive: true });
  const safeFileName = (fileName || 'MaStR_units').replace(/[\\/:*?"<>|]/g, '_');
  const filePath = path.join(saveFolder, safeFileName + '.xlsx');

  const allData = [];
  let browser = null;
  let context = null;

  // ── safeGoto ─────────────────────────────────────────────────────────────
  // No networkidle — every caller waits for a specific selector after this,
  // which is the real signal the content is ready.
  async function safeGoto(page, url, attempt) {
    attempt = attempt || 1;
    const MAX_ATTEMPTS = 3;
    try {
      await page.goto(url, { timeout: 120000, waitUntil: 'domcontentloaded' });
    } catch (err) {
      if (attempt < MAX_ATTEMPTS) {
        const delay = attempt * 3000;
        log('warn', 'goto attempt ' + attempt + ' failed — retry in ' + (delay/1000) + 's');
        await sleep(delay);
        return safeGoto(page, url, attempt + 1);
      }
      throw err;
    }
  }

  // ── withRetry ─────────────────────────────────────────────────────────────
  async function withRetry(label, fn, maxAttempts) {
    maxAttempts = maxAttempts || 3;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        return await fn();
      } catch (err) {
        if (attempt < maxAttempts) {
          const delay = attempt * 4000;
          log('warn', '[' + label + '] attempt ' + attempt + ' failed — retry in ' + (delay/1000) + 's: ' + err.message);
          await sleep(delay);
        } else {
          log('error', '[' + label + '] all attempts failed: ' + err.message);
          throw err;
        }
      }
    }
  }

  // ── collectAllDetailUrlsOnPage ────────────────────────────────────────────
  // NO scrolling — MaStR shows exactly the current page's 10 rows in the DOM.
  // Scrolling the grid container triggers virtual scroll and bleeds in rows
  // from the next page, causing 14–16 results instead of 10.
  async function collectAllDetailUrlsOnPage(page) {
    await page.waitForSelector(
      '.k-grid-content tbody tr.k-master-row[data-uid]',
      { timeout: 90000 }
    );

    const rows = await page.$$('.k-grid-content tbody tr.k-master-row[data-uid]');
    const detailUrls = [];

    for (const row of rows) {
      const link = await row.$('a.js-grid-detail');
      if (link) {
        const href = await link.getAttribute('href');
        if (href) detailUrls.push(BASE + href);
      }
    }

    log('info', 'Collected ' + detailUrls.length + ' URLs');
    return detailUrls;
  }

  // ── scrapeAllgemeineDaten ─────────────────────────────────────────────────
  async function scrapeAllgemeineDaten(context, url) {
    const page = await context.newPage();
    try {
      await withRetry('AllgemeineDaten', async () => {
        await safeGoto(page, url);
        // Wait for the actual data rows — not just the container
        await page.waitForSelector('div.panel-body table tr', { timeout: 90000 });
      });

      const data = {};
      const rows = await page.$$('div.panel-body table tr');

      for (const row of rows) {
        const cells = await row.$$('td');
        if (cells.length < 2) continue;
        const className = await row.getAttribute('class');
        let key = '';
        switch (className) {
          case 'detailstammdaten email': key = 'Anlagenbetreiber | Email';   break;
          case 'detailstammdaten phone': key = 'Anlagenbetreiber | Phone';   break;
          case 'detailstammdaten fax':   key = 'Anlagenbetreiber | Fax';     break;
          case 'detailstammdaten web':   key = 'Anlagenbetreiber | Website'; break;
          default: continue;
        }
        const val = (await cells[1].innerText()).trim();
        data[key] = val || '';
        log('info', key.padEnd(25) + ' : ' + val);
      }
      return data;
    } catch (e) {
      log('error', 'AllgemeineDaten failed: ' + e.message);
      return {};
    } finally {
      await page.close().catch(() => {});
    }
  }

  // ── scrapeDetailPage ──────────────────────────────────────────────────────
  async function scrapeDetailPage(context, url) {
    const page = await context.newPage();
    try {
      await withRetry('DetailPage', async () => {
        await safeGoto(page, url);
        // Wait for tabs to be visible — confirms page JS has executed
        await page.waitForSelector('ul.nav-tabs li a', { timeout: 90000 });
      });

      const data = { 'Detail URL': url };
      log('info', 'DETAIL: ' + url);

      const tabs = await page.$$('ul.nav-tabs li a');

      for (const tab of tabs) {
        let tabName = '';
        try {
          tabName = (await tab.innerText()).trim();
          await tab.click();

          // Wait for the active panel to appear
          await page.waitForSelector('div.tab-pane.active', { timeout: 30000 });

          // Wait for a table row inside the active panel — this is the AJAX completion signal.
          // If no row appears in 12s the tab is genuinely empty — skip it cleanly.
          try {
            await page.waitForSelector('div.tab-pane.active table tr', { timeout: 12000 });
          } catch (_) {
            log('info', 'Tab "' + tabName + '" has no table data — skipping');
            continue;
          }

          const panel = await page.$('div.tab-pane.active');
          if (!panel) continue;

          const tables = await panel.$$('table');
          for (const table of tables) {
            const rows = await table.$$('tr');
            for (const row of rows) {
              const cells = await row.$$('td');
              if (cells.length < 2) continue;
              const key = (await cells[0].innerText()).trim().replace(/:$/, '');
              const val = (await cells[1].innerText()).trim();
              if (key) data[tabName + ' | ' + key] = val || '';

              if (tabName === 'Allgemeine Daten' && key.includes('Anlagenbetreiber der Einheit')) {
                const linkEl = await cells[1].$('a');
                if (linkEl) {
                  const linkHref = await linkEl.getAttribute('href');
                  if (linkHref) {
                    const anlagenData = await scrapeAllgemeineDaten(context, BASE + linkHref);
                    Object.assign(data, anlagenData);
                  }
                }
              }
            }
          }
        } catch (tabErr) {
          log('warn', 'Tab "' + tabName + '" error (skipping): ' + tabErr.message);
        }
      }

      return data;
    } catch (e) {
      log('error', 'Detail page failed: ' + url + ' — ' + e.message);
      return { 'Detail URL': url, 'Error': e.message };
    } finally {
      await page.close().catch(() => {});
    }
  }

  // ── saveCleanExcel ────────────────────────────────────────────────────────
  function saveCleanExcel() {
    if (!allData.length) return;
    const headersSet = new Set();
    allData.forEach((row) => Object.keys(row).forEach((k) => headersSet.add(k)));
    const headers = Array.from(headersSet);
    const formattedData = allData.map((row) => {
      const r = {};
      headers.forEach((h) => (r[h] = row[h] || ''));
      return r;
    });
    const ws = xlsx.utils.json_to_sheet(formattedData, { header: headers });
    const wb = xlsx.utils.book_new();
    xlsx.utils.book_append_sheet(wb, ws, 'MaStR');
    ws['!cols'] = headers.map((h) => ({ wch: Math.max(h.length + 2, 15) }));
    xlsx.writeFile(wb, filePath);
    log('success', 'Saved: ' + filePath + ' (' + formattedData.length + ' rows)');
  }

  // ── runWithConcurrency ────────────────────────────────────────────────────
  async function runWithConcurrency(tasks, limit, handler) {
    const results = [];
    let idx = 0;
    async function worker() {
      while (idx < tasks.length) {
        const current = idx++;
        results[current] = await handler(tasks[current], current);
      }
    }
    await Promise.all(Array.from({ length: limit }, () => worker()));
    return results;
  }

  // ── MAIN ──────────────────────────────────────────────────────────────────
  try {
    log('info', 'MAX_PAGES=' + MAX_PAGES + ' CONCURRENCY=' + CONCURRENCY);

    let executablePath;
    try {
      executablePath = findChromiumExecutable();
      log('info', 'Chromium: ' + executablePath);
    } catch (e) {
      log('error', e.message);
      onDone({ success: false, error: e.message, filePath: null, rowCount: 0, stopped: false });
      return;
    }

    browser = await chromium.launch({
      headless: false,
      executablePath,
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
    });

    context = await browser.newContext({
      navigationTimeout: 120000,
      actionTimeout: 60000,
    });

    const page = await context.newPage();

    log('info', 'Loading listing page...');
    await safeGoto(page, LISTING_URL);

    // Wait for reload button, click it, then wait for grid rows — no fixed sleep
    await page.waitForSelector('button.gridReloadBtn', { timeout: 90000 });
    await page.click('button.gridReloadBtn');
    await page.waitForSelector('.k-grid-content tbody tr.k-master-row[data-uid]', { timeout: 90000 });

    let pageNum = 1;

    while (pageNum <= MAX_PAGES) {
      if (stopSignal.stopped) break;

      log('info', 'PAGE ' + pageNum + '/' + MAX_PAGES);
      onProgress({
        page: pageNum, maxPages: MAX_PAGES,
        pct: ((pageNum - 1) / MAX_PAGES) * 100,
        label: 'Page ' + pageNum + '/' + MAX_PAGES + ' — collecting listings...',
        totalRows: allData.length,
      });

      const detailUrls = await collectAllDetailUrlsOnPage(page);
      if (stopSignal.stopped) break;

      let scraped = 0;
      await runWithConcurrency(detailUrls, CONCURRENCY, async (url, i) => {
        if (stopSignal.stopped) return;
        log('info', '[' + (i+1) + '/' + detailUrls.length + '] ' + url);
        try {
          const row = await scrapeDetailPage(context, url);
          allData.push(row);
          onRow(row);
          scraped++;
          onProgress({
            page: pageNum, maxPages: MAX_PAGES,
            pct: ((pageNum - 1) / MAX_PAGES + (scraped / detailUrls.length) / MAX_PAGES) * 100,
            label: 'Page ' + pageNum + '/' + MAX_PAGES + ' — row ' + scraped + '/' + detailUrls.length,
            totalRows: allData.length,
          });
        } catch (e) {
          log('error', 'Worker skipped: ' + e.message);
        }
      });

      saveCleanExcel();

      if (stopSignal.stopped || pageNum === MAX_PAGES) {
        log('info', 'Stopping at page ' + pageNum + '.');
        break;
      }

      const nextBtn = await page.$('button[aria-label="N\u00e4chste Seite"]:not([aria-disabled="true"])');
      if (!nextBtn) { log('info', 'No next page found. Done.'); break; }

      // Snapshot the first row uid before clicking next.
      // After click we wait until that uid is gone — confirming the grid has
      // actually changed to the new page, not just started loading it.
      const firstUidBefore = await page.evaluate(function() {
        var row = document.querySelector('.k-grid-content tbody tr.k-master-row[data-uid]');
        return row ? row.getAttribute('data-uid') : null;
      });

      await nextBtn.click();

      // Wait for grid to switch to the new page
      if (firstUidBefore) {
        await page.waitForFunction(
          function(uid) {
            var row = document.querySelector('.k-grid-content tbody tr.k-master-row[data-uid]');
            return row && row.getAttribute('data-uid') !== uid;
          },
          firstUidBefore,
          { timeout: 90000 }
        );
      } else {
        await page.waitForSelector('.k-grid-content tbody tr.k-master-row[data-uid]', { timeout: 90000 });
      }

      pageNum++;
    }

    await context.close().catch(() => {});
    await browser.close().catch(() => {});

    const stopped = stopSignal.stopped;
    log(stopped ? 'warn' : 'success',
      stopped
        ? 'Stopped by user — ' + allData.length + ' rows saved.'
        : 'Complete! ' + allData.length + ' rows saved to ' + filePath);

    onDone({ success: true, filePath: allData.length ? filePath : null, rowCount: allData.length, stopped });

  } catch (e) {
    log('error', 'FATAL: ' + e.message);
    if (context) await context.close().catch(() => {});
    if (browser)  await browser.close().catch(() => {});
    if (allData.length) saveCleanExcel();
    // No rethrow — onDone handles the error, rethrowing causes double-done
    onDone({ success: false, error: e.message, filePath: allData.length ? filePath : null, rowCount: allData.length, stopped: false });
  }
};
