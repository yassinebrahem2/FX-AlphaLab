const { app, BrowserWindow } = require("electron");
const path = require("path");

const DEV_URL = "http://localhost:3000";
const isProd = app.isPackaged;

function createWindow() {
  const win = new BrowserWindow({
    width: 1600,
    height: 960,
    minWidth: 1200,
    minHeight: 700,
    title: "FX AlphaLab",
    frame: true,
    autoHideMenuBar: true,
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
    },
  });

  win.setMenuBarVisibility(false);

  if (isProd) {
    win.loadFile(path.join(__dirname, "../out/index.html"));
  } else {
    win.loadURL(DEV_URL);
  }
}

app.whenReady().then(createWindow);

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") app.quit();
});

app.on("activate", () => {
  if (BrowserWindow.getAllWindows().length === 0) createWindow();
});
