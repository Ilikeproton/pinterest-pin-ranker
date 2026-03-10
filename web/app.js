const {
  formatNumber,
  request,
  shortUrl,
  showNotice,
  signalDesktopReady,
  t,
  translateTree,
} = window.PinPulseUI;

const el = {
  globalStatus: document.getElementById("globalStatus"),
  globalStartBtn: document.getElementById("globalStartBtn"),
  globalStopBtn: document.getElementById("globalStopBtn"),
  concurrencyInput: document.getElementById("concurrencyInput"),
  defaultBatchMaxImagesInput: document.getElementById("defaultBatchMaxImagesInput"),
  saveSettingsBtn: document.getElementById("saveSettingsBtn"),
  refreshBtn: document.getElementById("refreshBtn"),
  archiveLimitInput: document.getElementById("archiveLimitInput"),
  archiveDoneBtn: document.getElementById("archiveDoneBtn"),
  runtimeStats: document.getElementById("runtimeStats"),
  createBatchForm: document.getElementById("createBatchForm"),
  batchName: document.getElementById("batchName"),
  seedUrl: document.getElementById("seedUrl"),
  threshold: document.getElementById("threshold"),
  maxImages: document.getElementById("maxImages"),
  maxDepth: document.getElementById("maxDepth"),
  batchWall: document.getElementById("batchWall"),
  emptyTip: document.getElementById("emptyTip"),
  batchCardTpl: document.getElementById("batchCardTpl"),
};

const state = {
  dashboard: null,
};

function batchLabel(batch) {
  return (batch.name || t("common.batchFallback", { id: batch.id })).trim();
}

function renderStats(stats = {}) {
  const pairs = [
    ["stats.totalPins", stats.total_pins || 0],
    ["stats.downloadedPins", stats.downloaded_pins || 0],
    ["stats.pendingTasks", stats.pending_tasks || 0],
    ["stats.doingTasks", stats.doing_tasks || 0],
    ["stats.errorTasks", stats.error_tasks || 0],
    ["stats.doneTasks", stats.done_tasks || 0],
  ];

  el.runtimeStats.innerHTML = "";
  for (const [labelKey, value] of pairs) {
    const card = document.createElement("div");
    card.className = "stat-chip";

    const strong = document.createElement("strong");
    strong.textContent = formatNumber(value);

    const span = document.createElement("span");
    span.textContent = t(labelKey);

    card.append(strong, span);
    el.runtimeStats.appendChild(card);
  }
}

function setGlobalBadge(running) {
  if (!el.globalStatus) {
    return;
  }
  el.globalStatus.textContent = t(running ? "dashboard.global.running" : "dashboard.global.stopped");
  el.globalStatus.className = running ? "status-chip status-chip--running" : "status-chip status-chip--idle";
}

function makeMini(labelKey, value) {
  return `<div class="mini-item"><strong>${value}</strong><span>${t(labelKey)}</span></div>`;
}

function renderBatches(items) {
  el.batchWall.innerHTML = "";
  if (!items || items.length === 0) {
    el.emptyTip.classList.remove("is-hidden");
    return;
  }
  el.emptyTip.classList.add("is-hidden");

  items.forEach((batch, index) => {
    const fragment = el.batchCardTpl.content.cloneNode(true);
    const root = fragment.querySelector(".batch-card");
    const coverImage = fragment.querySelector(".cover-image");
    const coverPlaceholder = fragment.querySelector(".cover-placeholder");
    const coverHeartPill = fragment.querySelector(".cover-heart-pill");
    const progressFill = fragment.querySelector(".batch-progress-fill");
    const progressCopy = fragment.querySelector(".batch-progress-copy");
    const runningNote = fragment.querySelector(".batch-running-note");
    const openBtn = fragment.querySelector(".btn-open");
    const toggleRunBtn = fragment.querySelector(".btn-toggle-run");
    const saved = Number(batch.saved || 0);
    const maxImages = Number(batch.max_images || 0);
    const progress = maxImages > 0 ? Math.min(100, Math.round((saved / maxImages) * 100)) : 0;

    root.style.animationDelay = `${Math.min(index * 40, 320)}ms`;
    root.tabIndex = 0;

    fragment.querySelector(".batch-name").textContent = batchLabel(batch);
    fragment.querySelector(".batch-seed").textContent = shortUrl(batch.seed_url);
    coverHeartPill.textContent = t("dashboard.batch.topHearts", {
      count: formatNumber(batch.top_hearts || 0),
    });
    progressCopy.textContent = t("dashboard.batch.progress", {
      saved: formatNumber(saved),
      max: formatNumber(maxImages),
    });
    progressFill.style.width = `${progress}%`;

    runningNote.textContent = t(
      batch.is_running ? "dashboard.batch.card.running" : "dashboard.batch.card.stopped"
    );
    runningNote.className = batch.is_running ? "batch-running-note is-running" : "batch-running-note is-stopped";

    fragment.querySelector(".mini-stats").innerHTML =
      makeMini("dashboard.batch.stat.topHearts", formatNumber(batch.top_hearts || 0)) +
      makeMini("dashboard.batch.stat.discovered", formatNumber(batch.discovered || 0)) +
      makeMini("dashboard.batch.stat.scanned", formatNumber(batch.scanned || 0)) +
      makeMini("dashboard.batch.stat.depth", formatNumber(batch.max_depth || 3)) +
      makeMini("dashboard.batch.stat.pending", formatNumber(batch.pending || 0)) +
      makeMini("dashboard.batch.stat.savedRate", `${progress}%`);

    if (batch.cover_url) {
      coverImage.src = batch.cover_url;
      coverImage.alt = `${batchLabel(batch)} cover`;
      coverImage.classList.remove("is-hidden");
      coverPlaceholder.classList.add("is-hidden");
    } else {
      coverImage.classList.add("is-hidden");
      coverPlaceholder.classList.remove("is-hidden");
    }

    const openDetail = () => {
      window.location.href = `/batch/${batch.id}`;
    };

    root.addEventListener("click", (event) => {
      if (event.target.closest("button")) return;
      openDetail();
    });
    root.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        openDetail();
      }
    });

    openBtn.textContent = t("dashboard.batch.open");
    openBtn.addEventListener("click", openDetail);

    toggleRunBtn.textContent = t(batch.is_running ? "dashboard.batch.stop" : "dashboard.batch.start");
    toggleRunBtn.className = batch.is_running
      ? "button button--ghost button--danger btn-toggle-run"
      : "button button--accent btn-toggle-run";

    toggleRunBtn.addEventListener("click", async (event) => {
      event.stopPropagation();
      try {
        const endpoint = batch.is_running ? "stop" : "start";
        await request(`/api/batches/${batch.id}/${endpoint}`, { method: "POST" });
        showNotice(
          t(batch.is_running ? "dashboard.notice.batchStopped" : "dashboard.notice.batchStarted"),
          "success"
        );
        await loadDashboard();
      } catch (err) {
        showNotice(err.message, "error");
      }
    });

    translateTree(fragment);
    el.batchWall.appendChild(fragment);
  });
}

function renderDashboard(data) {
  state.dashboard = data;
  document.title = t("dashboard.title");

  if (el.concurrencyInput) {
    el.concurrencyInput.value = data.settings?.concurrency || 3;
  }
  if (el.defaultBatchMaxImagesInput) {
    el.defaultBatchMaxImagesInput.value = data.settings?.default_batch_max_images || 100;
  }
  if (el.threshold) {
    el.threshold.value = data.settings?.default_threshold || 2;
  }
  if (el.maxImages && !el.maxImages.dataset.userEdited) {
    el.maxImages.value = data.settings?.default_batch_max_images || 100;
  }
  if (el.maxDepth && !el.maxDepth.dataset.userEdited) {
    el.maxDepth.value = "3";
  }

  setGlobalBadge(Boolean(data.settings?.global_running));
  renderStats(data.stats || {});
  renderBatches(data.batches || []);
  translateTree(document);
}

async function loadDashboard() {
  const data = await request("/api/dashboard");
  renderDashboard(data);
}

function bindEvents() {
  el.maxImages?.addEventListener("input", () => {
    el.maxImages.dataset.userEdited = "1";
  });

  el.maxDepth?.addEventListener("input", () => {
    el.maxDepth.dataset.userEdited = "1";
  });

  el.saveSettingsBtn?.addEventListener("click", async () => {
    const concurrency = Number(el.concurrencyInput?.value || 3);
    const default_batch_max_images = Number(el.defaultBatchMaxImagesInput?.value || 100);
    try {
      await request("/api/settings", {
        method: "PUT",
        body: JSON.stringify({ concurrency, default_batch_max_images }),
      });
      if (el.maxImages && !el.maxImages.dataset.userEdited) {
        el.maxImages.value = String(default_batch_max_images);
      }
      showNotice(t("dashboard.notice.settingsSaved"), "success");
      await loadDashboard();
    } catch (err) {
      showNotice(err.message, "error");
    }
  });

  el.refreshBtn?.addEventListener("click", async () => {
    try {
      await loadDashboard();
      showNotice(t("dashboard.notice.refreshed"), "info");
    } catch (err) {
      showNotice(err.message, "error");
    }
  });

  el.archiveDoneBtn?.addEventListener("click", async () => {
    const limit = Number(el.archiveLimitInput?.value || 10000);
    try {
      const data = await request("/api/maintenance/archive-done", {
        method: "POST",
        body: JSON.stringify({ limit }),
      });
      await loadDashboard();
      showNotice(
        t("dashboard.notice.archiveDone", {
          count: formatNumber(data.archived || 0),
        }),
        "success"
      );
    } catch (err) {
      showNotice(err.message, "error");
    }
  });

  el.globalStartBtn?.addEventListener("click", async () => {
    try {
      await request("/api/control/start", { method: "POST" });
      showNotice(t("dashboard.notice.globalStarted"), "success");
      await loadDashboard();
    } catch (err) {
      showNotice(err.message, "error");
    }
  });

  el.globalStopBtn?.addEventListener("click", async () => {
    try {
      await request("/api/control/stop", { method: "POST" });
      showNotice(t("dashboard.notice.globalStopped"), "info");
      await loadDashboard();
    } catch (err) {
      showNotice(err.message, "error");
    }
  });

  el.createBatchForm?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const name = (el.batchName?.value || "").trim();
    const seed_url = (el.seedUrl?.value || "").trim();
    const threshold = Number(el.threshold?.value || 2);
    const max_images = Number(el.maxImages?.value || 100);
    const max_depth = Number(el.maxDepth?.value || 3);

    try {
      await request("/api/batches", {
        method: "POST",
        body: JSON.stringify({ name, seed_url, threshold, max_images, max_depth }),
      });
      if (el.batchName) el.batchName.value = "";
      if (el.seedUrl) el.seedUrl.value = "";
      if (el.maxImages) el.maxImages.dataset.userEdited = "";
      if (el.maxDepth) el.maxDepth.dataset.userEdited = "";
      showNotice(t("dashboard.notice.batchCreated"), "success");
      await loadDashboard();
    } catch (err) {
      showNotice(err.message, "error");
    }
  });

  document.addEventListener("pinpulse:locale-changed", () => {
    if (state.dashboard) {
      renderDashboard(state.dashboard);
    } else {
      translateTree(document);
    }
  });
}

async function init() {
  bindEvents();
  await loadDashboard();
}

init()
  .then(() => {
    signalDesktopReady();
  })
  .catch((err) => {
    signalDesktopReady();
    showNotice(err.message, "error");
  });
