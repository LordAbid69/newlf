// preload.js — context bridge (renderer ↔ main)
'use strict';

const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('api', {
  // ── Window controls ──────────────────────────────────────────────────────
  minimize: () => ipcRenderer.send('win:minimize'),
  maximize: () => ipcRenderer.send('win:maximize'),
  close:    () => ipcRenderer.send('win:close'),

  // ── Dialogs / OS ─────────────────────────────────────────────────────────
  pickFolder:   ()         => ipcRenderer.invoke('dialog:pickFolder'),
  getDocsPath:  ()         => ipcRenderer.invoke('app:docsPath'),
  showFile:     (filePath) => ipcRenderer.send('shell:showFile', filePath),

  // ── Scraper control ───────────────────────────────────────────────────────
  startScraper: (config) => ipcRenderer.send('scraper:start', config),
  stopScraper:  ()       => ipcRenderer.send('scraper:stop'),

  // ── Scraper events (renderer listeners) ──────────────────────────────────
  onRow:      (cb) => ipcRenderer.on('scraper:row',      (_, d) => cb(d)),
  onLog:      (cb) => ipcRenderer.on('scraper:log',      (_, d) => cb(d)),
  onProgress: (cb) => ipcRenderer.on('scraper:progress', (_, d) => cb(d)),
  onDone:     (cb) => ipcRenderer.on('scraper:done',     (_, d) => cb(d)),
  onError:    (cb) => ipcRenderer.on('scraper:error',    (_, d) => cb(d)),

  // ── Clean up listeners when starting a fresh run ─────────────────────────
  removeAllListeners: (channel) => ipcRenderer.removeAllListeners(channel),
});
