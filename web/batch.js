const batchId = Number((window.location.pathname.split("/").pop() || "").trim());
const {
  formatNumber,
  locateImage,
  prettyDate,
  request,
  showNotice,
  signalDesktopReady,
  t,
  translateTree,
} = window.PinPulseUI;

const el = {
  batchTitle: document.getElementById("batchTitle"),
  batchMeta: document.getElementById("batchMeta"),
  batchRunningBadge: document.getElementById("batchRunningBadge"),
  batchStartBtn: document.getElementById("batchStartBtn"),
  batchStopBtn: document.getElementById("batchStopBtn"),
  batchDeleteBtn: document.getElementById("batchDeleteBtn"),
  reloadPinsBtn: document.getElementById("reloadPinsBtn"),
  viewMode: document.getElementById("viewMode"),
  limitCount: document.getElementById("limitCount"),
  batchThresholdInput: document.getElementById("batchThresholdInput"),
  batchMaxDepthInput: document.getElementById("batchMaxDepthInput"),
  saveThresholdBtn: document.getElementById("saveThresholdBtn"),
  pinWall: document.getElementById("pinWall"),
  pinEmptyTip: document.getElementById("pinEmptyTip"),
  pinCardTpl: document.getElementById("pinCardTpl"),
};

const state = {
  batch: null,
  pins: [],
};

function renderHeartPill(node, hearts) {
  if (!node) {
    return;
  }

  const count = formatNumber(hearts || 0);
  const icon = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  icon.setAttribute("viewBox", "0 0 24 24");
  icon.setAttribute("aria-hidden", "true");
  icon.classList.add("pin-heart-pill__icon");

  const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
  path.setAttribute(
    "d",
    "M12 21.35l-1.45-1.32C5.4 15.36 2 12.28 2 8.5 2 5.42 4.42 3 7.5 3c1.74 0 3.41.81 4.5 2.09C13.09 3.81 14.76 3 16.5 3 19.58 3 22 5.42 22 8.5c0 3.78-3.4 6.86-8.55 11.54z",
  );
  icon.appendChild(path);

  const value = document.createElement("span");
  value.textContent = count;

  node.replaceChildren(icon, value);
  node.setAttribute("aria-label", t("detail.pin.hearts", { count }));
  node.setAttribute("title", t("detail.pin.hearts", { count }));
}

function batchLabel(batch) {
  return (batch?.name || t("common.batchFallback", { id: batch?.id || "" })).trim();
}

function setBatchRunningBadge(running) {
  if (!el.batchRunningBadge) {
    return;
  }
  el.batchRunningBadge.textContent = t(running ? "detail.running" : "detail.stopped");
  el.batchRunningBadge.className = running ? "status-chip status-chip--running" : "status-chip status-chip--idle";
}

function pinStatus(pin) {
  if (pin.included) {
    return t("detail.pin.status.included");
  }
  if (state.batch && Number(pin.hearts || 0) >= Number(state.batch.threshold || 0)) {
    return t("detail.pin.status.capped");
  }
  return t("detail.pin.status.linkOnly");
}

function pinLevel(pin) {
  const level = Number(pin.relation_level || 0);
  if (!Number.isFinite(level) || level <= 0 || level >= 999999) {
    return t("detail.pin.levelUnknown");
  }
  return t("detail.pin.level", { level: formatNumber(level) });
}

function renderBatch(batch) {
  state.batch = batch;
  document.title = t("detail.pageTitle", { name: batchLabel(batch) });

  if (el.batchTitle) {
    el.batchTitle.textContent = `${batchLabel(batch)} (#${batch.id})`;
  }
  if (el.batchMeta) {
    el.batchMeta.textContent = t("detail.meta", {
      seed: batch.seed_url,
      threshold: formatNumber(batch.threshold),
      maxDepth: formatNumber(batch.max_depth || 3),
      topHearts: formatNumber(batch.top_hearts || 0),
      saved: formatNumber(batch.saved),
      maxImages: formatNumber(batch.max_images),
      discovered: formatNumber(batch.discovered),
      scanned: formatNumber(batch.scanned),
      pending: formatNumber(batch.pending),
    });
  }
  if (el.batchThresholdInput) {
    el.batchThresholdInput.value = String(batch.threshold || 2);
  }
  if (el.batchMaxDepthInput) {
    el.batchMaxDepthInput.value = String(batch.max_depth || 3);
  }
  setBatchRunningBadge(Boolean(batch.is_running));
  translateTree(document);
}

function renderPins(items) {
  state.pins = items || [];
  el.pinWall.innerHTML = "";
  if (!items || items.length === 0) {
    el.pinEmptyTip.classList.remove("is-hidden");
    return;
  }
  el.pinEmptyTip.classList.add("is-hidden");

  items.forEach((pin, index) => {
    const fragment = el.pinCardTpl.content.cloneNode(true);
    const img = fragment.querySelector(".pin-image");
    const media = fragment.querySelector(".pin-media");
    const heartPill = fragment.querySelector(".pin-heart-pill");
    const placeholder = fragment.querySelector(".pin-placeholder");
    const titleNode = fragment.querySelector(".pin-title");
    const statusNode = fragment.querySelector(".pin-status");
    const body = fragment.querySelector(".pin-body");
    const link = fragment.querySelector(".pin-link");
    const locateBtn = fragment.querySelector(".pin-locate");
    const root = fragment.querySelector(".pin-card");

    root.style.animationDelay = `${Math.min(index * 32, 240)}ms`;
    media.href = pin.url;
    link.href = pin.url;

    titleNode.textContent = pin.title || t("common.pinFallback", { id: pin.pin_id });
    renderHeartPill(heartPill, pin.hearts || 0);
    statusNode.textContent = `${pinLevel(pin)} | ${pinStatus(pin)}`;

    if (pin.image_view_url) {
      img.src = pin.image_view_url;
      img.alt = pin.title || "pin image";
      img.classList.remove("is-hidden");
      placeholder.classList.add("is-hidden");
    } else {
      img.classList.add("is-hidden");
      placeholder.classList.remove("is-hidden");
    }

    if (pin.last_checked_at) {
      const checked = document.createElement("p");
      checked.className = "pin-status";
      checked.textContent = t("detail.pin.checkedAt", {
        value: prettyDate(pin.last_checked_at),
      });
      body.appendChild(checked);
    }

    locateBtn?.addEventListener("click", async () => {
      try {
        const located = await locateImage({
          sourcePath: pin.image_path || "",
          sourceURL: pin.image_view_url || pin.image_url || "",
        });
        if (located) {
          showNotice(t("detail.notice.imageLocated"), "info");
        }
      } catch (err) {
        showNotice(err.message || t("detail.notice.imageLocateFailed"), "error");
      }
    });

    translateTree(fragment);
    el.pinWall.appendChild(fragment);
  });
}

async function loadBatch() {
  const batch = await request(`/api/batches/${batchId}`);
  renderBatch(batch);
}

async function loadPins() {
  const mode = el.viewMode?.value || "included";
  const limit = Number(el.limitCount?.value || 800);
  const data = await request(`/api/batches/${batchId}/pins?mode=${encodeURIComponent(mode)}&limit=${limit}`);
  renderPins(data.items || []);
}

async function refreshAll() {
  await Promise.all([loadBatch(), loadPins()]);
}

function rerenderLocale() {
  if (state.batch) {
    renderBatch(state.batch);
    renderPins(state.pins);
  } else {
    document.title = t("detail.title");
    translateTree(document);
  }
}

function bindEvents() {
  el.batchStartBtn?.addEventListener("click", async () => {
    try {
      await request(`/api/batches/${batchId}/start`, { method: "POST" });
      showNotice(t("detail.notice.started"), "success");
      await refreshAll();
    } catch (err) {
      showNotice(err.message, "error");
    }
  });

  el.batchStopBtn?.addEventListener("click", async () => {
    try {
      await request(`/api/batches/${batchId}/stop`, { method: "POST" });
      showNotice(t("detail.notice.stopped"), "info");
      await refreshAll();
    } catch (err) {
      showNotice(err.message, "error");
    }
  });

  el.batchDeleteBtn?.addEventListener("click", async () => {
    const confirmed = window.confirm(t("detail.confirm.delete"));
    if (!confirmed) {
      return;
    }

    try {
      await request(`/api/batches/${batchId}/stop`, { method: "POST" }).catch(() => {});
      await request(`/api/batches/${batchId}`, { method: "DELETE" });
      window.location.href = "/";
    } catch (err) {
      showNotice(err.message, "error");
    }
  });

  el.reloadPinsBtn?.addEventListener("click", async () => {
    try {
      await refreshAll();
      showNotice(t("detail.notice.refreshed"), "info");
    } catch (err) {
      showNotice(err.message, "error");
    }
  });

  el.viewMode?.addEventListener("change", async () => {
    try {
      await loadPins();
    } catch (err) {
      showNotice(err.message, "error");
    }
  });

  el.limitCount?.addEventListener("change", async () => {
    try {
      await loadPins();
    } catch (err) {
      showNotice(err.message, "error");
    }
  });

  el.saveThresholdBtn?.addEventListener("click", async () => {
    const threshold = Number(el.batchThresholdInput?.value || 0);
    const max_depth = Number(el.batchMaxDepthInput?.value || 0);
    if (!Number.isFinite(threshold) || threshold < 1) {
      showNotice(t("detail.error.thresholdMin"), "error");
      return;
    }
    if (!Number.isFinite(max_depth) || max_depth < 1) {
      showNotice(t("detail.error.maxDepthMin"), "error");
      return;
    }
    try {
      await request(`/api/batches/${batchId}`, {
        method: "PUT",
        body: JSON.stringify({ threshold, max_depth }),
      });
      showNotice(t("detail.notice.rulesUpdated"), "success");
      await refreshAll();
    } catch (err) {
      showNotice(err.message, "error");
    }
  });

  document.addEventListener("pinpulse:locale-changed", rerenderLocale);
}

async function init() {
  if (!batchId || Number.isNaN(batchId)) {
    throw new Error(t("detail.error.invalidBatchId"));
  }
  document.title = t("detail.title");
  translateTree(document);
  bindEvents();
  await refreshAll();
}

init()
  .then(() => {
    signalDesktopReady();
  })
  .catch((err) => {
    signalDesktopReady();
    showNotice(err.message, "error");
  });
