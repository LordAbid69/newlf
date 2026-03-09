// scraper.js
'use strict';

function findChromiumExecutable() {
  var fs   = require('fs');
  var path = require('path');

  var browsersRoot = process.env.PLAYWRIGHT_BROWSERS_PATH;
  if (!browsersRoot) {
    if (process.resourcesPath) {
      browsersRoot = path.join(process.resourcesPath, 'browsers');
    } else {
      browsersRoot = path.join(__dirname, 'browsers');
    }
  }

  if (!fs.existsSync(browsersRoot)) {
    throw new Error('Browsers folder not found: ' + browsersRoot + '\nRun BUILD.bat to download Chromium.');
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

  var exePath = walk(browsersRoot);
  if (!exePath) {
    var tree = '';
    try {
      fs.readdirSync(browsersRoot).forEach(function(e) {
        tree += e + '\n';
        try {
          fs.readdirSync(path.join(browsersRoot, e)).forEach(function(f) { tree += '    ' + f + '\n'; });
        } catch (_) {}
      });
    } catch (_) {}
    throw new Error('chrome.exe not found inside: ' + browsersRoot + '\nContents:\n' + tree + '\nRun BUILD.bat again.');
  }

  return exePath;
}

module.exports = async function runScraper(config, callbacks, stopSignal) {
  const { chromium } = require('playwright-core');
  const xlsx  = require('xlsx');
  const fs    = require('fs');
  const path  = require('path');

  const {
    url: LISTING_URL,
    maxPages: MAX_PAGES,
    concurrency: CONCURRENCY,
    saveFolder,
    fileName,
  } = config;

  const { onLog, onRow, onProgress, onDone } = callbacks;

  let BASE;
  try {
    const u = new URL(LISTING_URL);
    BASE = u.protocol + '//' + u.host;
  } catch (_) {
    BASE = 'https://www.marktstammdatenregister.de';
  }

  const log   = (level, text) => { onLog(level, text); };
  const sleep = (ms) => new Promise((res) => setTimeout(res, ms));

  if (!fs.existsSync(saveFolder)) fs.mkdirSync(saveFolder, { recursive: true });
  const safeFileName = (fileName || 'MaStR_units').replace(/[\\/:*?"<>|]/g, '_');
  const filePath = path.join(saveFolder, safeFileName + '.xlsx');

  const allData = [];
  let browser = null;
  let context = null;

  // ================= SAFE GOTO — matches original exactly =================
  async function safeGoto(page, url, attempt) {
    attempt = attempt || 1;
    const MAX_ATTEMPTS = 3;
    const TIMEOUT = 120000;

    try {
      await page.goto(url, { timeout: TIMEOUT, waitUntil: 'domcontentloaded' });

      try {
        await page.waitForLoadState('networkidle', { timeout: 30000 });
      } catch (_) {
        log('warn', 'networkidle timeout (non-fatal), continuing...');
      }

      await sleep(500);
    } catch (err) {
      if (attempt < MAX_ATTEMPTS) {
        const delay = attempt * 3000;
        log('warn', 'Load attempt ' + attempt + '/' + MAX_ATTEMPTS + ' failed — retrying in ' + (delay / 1000) + 's... (' + url + ')');
        await sleep(delay);
        return safeGoto(page, url, attempt + 1);
      }
      log('error', 'All ' + MAX_ATTEMPTS + ' load attempts failed: ' + url);
      throw err;
    }
  }

  // ================= GENERIC RETRY WRAPPER — matches original exactly =================
  async function withRetry(label, fn, maxAttempts) {
    maxAttempts = maxAttempts || 3;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        return await fn();
      } catch (err) {
        if (attempt < maxAttempts) {
          const delay = attempt * 4000;
          log('warn', '[' + label + '] Attempt ' + attempt + '/' + maxAttempts + ' failed: ' + err.message + ' — retrying in ' + (delay / 1000) + 's');
          await sleep(delay);
        } else {
          log('error', '[' + label + '] All ' + maxAttempts + ' attempts failed: ' + err.message);
          throw err;
        }
      }
    }
  }

  // ================= COLLECT URLs — matches original exactly =================
  async function collectAllDetailUrlsOnPage(page) {
    await page.waitForSelector('.k-grid-content tbody tr.k-master-row[data-uid]', { timeout: 90000 });

    const grid = await page.$('.k-grid-content');
    const seen = new Set();
    const detailUrls = [];
    let noProgressStreak = 0;

    while (true) {
      const rows = await page.$$('.k-grid-content tbody tr.k-master-row[data-uid]');
      const before = seen.size;

      for (const row of rows) {
        const uid = await row.getAttribute('data-uid');
        if (!uid || seen.has(uid)) continue;
        seen.add(uid);

        const link = await page.$('tr[data-uid="' + uid + '"] a.js-grid-detail');
        if (link) {
          const href = await link.getAttribute('href');
          if (href) detailUrls.push(BASE + href);
        }
      }

      await page.evaluate((el) => (el.scrollTop = el.scrollHeight), grid);
      await sleep(900);

      if (seen.size === before) {
        noProgressStreak++;
        if (noProgressStreak >= 3) break;
      } else {
        noProgressStreak = 0;
      }
    }

    return detailUrls;
  }

  // ================= SCRAPE ALLGEMEINE DATEN — matches original exactly =================
  async function scrapeAllgemeineDaten(context, url) {
    const page = await context.newPage();
    try {
      await withRetry('AllgemeineDaten ' + url, async () => {
        await safeGoto(page, url);
        await page.waitForSelector('div.panel-body', { timeout: 90000 });
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
        log('info', key.padEnd(25, ' ') + ' : ' + val);
      }

      return data;
    } catch (e) {
      log('error', 'AllgemeineDaten error (giving up): ' + e.message);
      return {};
    } finally {
      await page.close().catch(() => {});
    }
  }

  // ================= SCRAPE DETAIL PAGE — matches original exactly =================
  async function scrapeDetailPage(context, url) {
    const page = await context.newPage();
    try {
      await withRetry('DetailPage ' + url, async () => {
        await safeGoto(page, url);
        await page.waitForSelector('ul.nav-tabs', { timeout: 90000 });
      });

      const data = { 'Detail URL': url };

      log('info', '==============================');
      log('info', 'DETAIL PAGE: ' + url);
      log('info', '==============================');

      const tabs = await page.$$('ul.nav-tabs li a');

      for (const tab of tabs) {
        let tabName = '';
        try {
          tabName = (await tab.innerText()).trim();
          await tab.click();

          await page.waitForSelector('div.tab-pane.active', { timeout: 30000 });
          try {
            await page.waitForLoadState('networkidle', { timeout: 20000 });
          } catch (_) {
            // Non-fatal — tab content is still readable
          }
          await sleep(400);

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
          continue;
        }
      }

      return data;
    } catch (e) {
      log('error', 'Detail page error (giving up): ' + url + ' ' + e.message);
      return { 'Detail URL': url, 'Error': e.message };
    } finally {
      await page.close().catch(() => {});
    }
  }

  // ================= SAVE EXCEL — matches original exactly =================
  function saveCleanExcel() {
    if (!allData.length) return;

    const headersSet = new Set();
    allData.forEach((row) => Object.keys(row).forEach((key) => headersSet.add(key)));
    const headers = Array.from(headersSet);

    const formattedData = allData.map((row) => {
      const newRow = {};
      headers.forEach((h) => (newRow[h] = row[h] || ''));
      return newRow;
    });

    const ws = xlsx.utils.json_to_sheet(formattedData, { header: headers });
    const wb = xlsx.utils.book_new();
    xlsx.utils.book_append_sheet(wb, ws, 'MaStR');
    ws['!cols'] = headers.map((h) => ({ wch: Math.max(h.length + 2, 15) }));

    xlsx.writeFile(wb, filePath);
    log('success', 'Auto-saved: ' + filePath);
    log('info', 'Total rows: ' + formattedData.length);
  }

  // ================= CONCURRENCY POOL — matches original exactly =================
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

  // ================= MAIN FLOW =================
  try {
    log('info', 'Finding Chromium executable...');
    let executablePath;
    try {
      executablePath = findChromiumExecutable();
      log('info', 'Chromium found: ' + executablePath);
    } catch (e) {
      log('error', e.message);
      onDone({ success: false, error: e.message, filePath: null, rowCount: 0, stopped: false });
      return;
    }

    log('info', 'Launching browser...');

    browser = await chromium.launch({
      headless: false,
      executablePath: executablePath,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
      ],
    });

    context = await browser.newContext({
      navigationTimeout: 120000,
      actionTimeout:      60000,
    });

    const page = await context.newPage();

    log('info', 'Loading: ' + LISTING_URL);
    await safeGoto(page, LISTING_URL);

    await page.waitForSelector('button.gridReloadBtn', { timeout: 90000 });
    await page.click('button.gridReloadBtn');
    await sleep(3000);

    let pageNum = 1;

    while (pageNum <= MAX_PAGES && !stopSignal.stopped) {
      log('info', 'PROCESSING PAGE ' + pageNum + '/' + MAX_PAGES);

      onProgress({
        page: pageNum,
        maxPages: MAX_PAGES,
        pct: ((pageNum - 1) / MAX_PAGES) * 100,
        label: 'Page ' + pageNum + '/' + MAX_PAGES + ' — collecting listings...',
        totalRows: allData.length,
      });

      const detailUrls = await collectAllDetailUrlsOnPage(page);
      log('info', 'Found ' + detailUrls.length + ' listings');

      if (stopSignal.stopped) break;

      let scraped = 0;

      await runWithConcurrency(detailUrls, CONCURRENCY, async (url, i) => {
        if (stopSignal.stopped) return;
        log('info', 'Scraping ' + (i + 1) + '/' + detailUrls.length + ': ' + url);
        try {
          const row = await scrapeDetailPage(context, url);
          allData.push(row);
          onRow(row);
          scraped++;
          onProgress({
            page: pageNum,
            maxPages: MAX_PAGES,
            pct: ((pageNum - 1) / MAX_PAGES + (scraped / detailUrls.length) / MAX_PAGES) * 100,
            label: 'Page ' + pageNum + '/' + MAX_PAGES + ' — row ' + scraped + '/' + detailUrls.length,
            totalRows: allData.length,
          });
        } catch (e) {
          log('error', 'Worker error (skipped): ' + e.message);
        }
      });

      saveCleanExcel();

      if (stopSignal.stopped || pageNum === MAX_PAGES) {
        log('info', 'Reached final page. Stopping.');
        break;
      }

      const nextBtn = await page.$('button[aria-label="N\u00e4chste Seite"]:not([aria-disabled="true"])');
      if (!nextBtn) {
        log('info', 'No next page button found. Stopping.');
        break;
      }

      await nextBtn.click();
      await sleep(3000);
      pageNum++;
    }

    await context.close().catch(() => {});
    await browser.close().catch(() => {});

    const stopped = stopSignal.stopped;
    log(
      stopped ? 'warn' : 'success',
      stopped
        ? 'Stopped by user — ' + allData.length + ' rows saved.'
        : 'Scraping complete! ' + allData.length + ' rows saved to ' + filePath
    );

    onDone({ success: true, filePath: allData.length ? filePath : null, rowCount: allData.length, stopped: stopped });

  } catch (e) {
    if (context) await context.close().catch(() => {});
    if (browser) await browser.close().catch(() => {});
    if (allData.length) saveCleanExcel();
    onDone({ success: false, error: e.message, filePath: allData.length ? filePath : null, rowCount: allData.length, stopped: false });
    throw e;
  }
};
