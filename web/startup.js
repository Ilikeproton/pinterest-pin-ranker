const { signalDesktopReady, t } = window.PinPulseUI;

const statusText = document.getElementById("statusText");
const targetURL = (() => {
  const raw = new URLSearchParams(window.location.search).get("target");
  if (raw) {
    return raw;
  }
  return window.location.origin;
})();

let retries = 0;
let currentStatusKey = "startup.status.booting";

function setStatus(key) {
  currentStatusKey = key;
  if (statusText) {
    statusText.textContent = t(key);
  }
}

function applyTitle() {
  document.title = t("startup.title");
  setStatus(currentStatusKey);
}

function swapToApp() {
  setStatus("startup.status.ready");
  window.setTimeout(() => {
    window.location.replace(targetURL);
  }, 200);
}

async function pollHealth() {
  try {
    const response = await fetch(`${targetURL}/api/health`, { cache: "no-store" });
    if (response.ok) {
      swapToApp();
      return;
    }
  } catch (_) {
    // Keep polling until the server is ready.
  }

  retries += 1;
  if (retries === 8) {
    setStatus("startup.status.connecting");
  } else if (retries === 16) {
    setStatus("startup.status.slower");
  }

  window.setTimeout(pollHealth, 160);
}

document.addEventListener("pinpulse:locale-changed", applyTitle);

applyTitle();
requestAnimationFrame(() => {
  signalDesktopReady();
});
pollHealth();
