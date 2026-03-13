// main.js — Electron main process
'use strict';

const { app, BrowserWindow, ipcMain, dialog, shell } = require('electron');
const path = require('path');
const os   = require('os');
const fs   = require('fs');

// ─── Set Playwright browsers path BEFORE anything else ──────────────────────
const browsersPath = app.isPackaged
  ? path.join(process.resourcesPath, 'browsers')
  : path.join(__dirname, 'browsers');
process.env.PLAYWRIGHT_BROWSERS_PATH = browsersPath;

// ─── Globals ─────────────────────────────────────────────────────────────────
let mainWindow  = null;
let stopSignal  = { stopped: false };
let isRunning   = false;

// ─── Create window ────────────────────────────────────────────────────────────
function createWindow() {
  mainWindow = new BrowserWindow({
    width:     1400,
    height:    860,
    minWidth:  1060,
    minHeight: 660,
    frame:     false,
    backgroundColor: '#0a0d12',
    webPreferences: {
      preload:          path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration:  false,
      sandbox:          false,
    },
    show: false,
  });

  mainWindow.loadFile('index.html');
  mainWindow.once('ready-to-show', () => mainWindow.show());
  mainWindow.on('closed', () => {
    stopSignal.stopped = true;
    mainWindow = null;
  });
}

app.whenReady().then(createWindow);
app.on('window-all-closed', () => { stopSignal.stopped = true; app.quit(); });

// ─── Safe renderer send ───────────────────────────────────────────────────────
function send(channel, data) {
  if (mainWindow && !mainWindow.isDestroyed()) {
    mainWindow.webContents.send(channel, data);
  }
}

// ─── Window controls ─────────────────────────────────────────────────────────
ipcMain.on('win:minimize', () => mainWindow?.minimize());
ipcMain.on('win:maximize', () => {
  if (!mainWindow) return;
  mainWindow.isMaximized() ? mainWindow.unmaximize() : mainWindow.maximize();
});
ipcMain.on('win:close', () => {
  stopSignal.stopped = true;
  mainWindow?.close();
});

// ─── Folder picker ────────────────────────────────────────────────────────────
ipcMain.handle('dialog:pickFolder', async () => {
  if (!mainWindow) return null;
  const result = await dialog.showOpenDialog(mainWindow, {
    properties: ['openDirectory'],
    defaultPath: path.join(os.homedir(), 'Documents'),
  });
  return result.canceled ? null : result.filePaths[0];
});

// ─── Defaults ────────────────────────────────────────────────────────────────
ipcMain.handle('app:docsPath', () => path.join(os.homedir(), 'Documents'));

// ─── Open file in Explorer ────────────────────────────────────────────────────
ipcMain.on('shell:showFile', (_, filePath) => shell.showItemInFolder(filePath));

// ─── Start scraper ────────────────────────────────────────────────────────────
ipcMain.on('scraper:start', async (_, config) => {
  if (isRunning) return;
  isRunning   = true;
  stopSignal  = { stopped: false };

  // Load scraper module — works both in dev and packaged (asar)
  let runScraper;
  try {
    // Clear require cache so each run gets a fresh module with fresh closures
    delete require.cache[require.resolve('./scraper.js')];
    runScraper = require('./scraper.js');
  } catch (e) {
    send('scraper:log', { level: 'error', text: `Failed to load scraper module: ${e.message}` });
    send('scraper:done', { success: false });
    isRunning = false;
    return;
  }

  try {
    await runScraper(
      config,
      {
        onLog:      (level, text) => send('scraper:log',      { level, text }),
        onRow:      (row)         => send('scraper:row',      row),
        onProgress: (data)        => send('scraper:progress', data),
        onDone:     (data)        => send('scraper:done',     data),
      },
      stopSignal,
    );
  } catch (e) {
    send('scraper:log',  { level: 'error', text: `Fatal error: ${e.message}` });
    send('scraper:done', { success: false, error: e.message });
  } finally {
    isRunning = false;
  }
});

// ─── Stop scraper ─────────────────────────────────────────────────────────────
ipcMain.on('scraper:stop', () => {
  stopSignal.stopped = true;
  send('scraper:log', { level: 'warn', text: '⏹  Stop requested — finishing current batch...' });
});
