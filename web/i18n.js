(function () {
  const STORAGE_KEY = "pinpulse.locale";
  const DEFAULT_LOCALE = "en";
  const LABELS = {
    en: "English",
    "zh-CN": "中文",
  };

  const messages = {
    en: {
      "dashboard.title": "PinPulse | Desktop Pinterest Workspace",
      "dashboard.hero.eyebrow": "Desktop Pinterest Workspace",
      "dashboard.hero.copy":
        "Start from a single Pinterest Pin, keep discovering related links, collect heart counts, and save qualified images into a local workspace.",
      "dashboard.pill.localFirst": "Local-first",
      "dashboard.pill.sqlite": "SQLite",
      "dashboard.pill.assets": "Pinned Assets",
      "dashboard.global.running": "Global Running",
      "dashboard.global.stopped": "Global Stopped",
      "dashboard.global.start": "Start All",
      "dashboard.global.stop": "Stop All",
      "dashboard.sections.settings.eyebrow": "Workspace",
      "dashboard.sections.settings.title": "System Settings",
      "dashboard.sections.settings.note":
        "Control global concurrency, default image retention, and task archiving.",
      "dashboard.settings.concurrency.label": "Concurrent Workers",
      "dashboard.settings.concurrency.help": "Recommended range: 1-8.",
      "dashboard.settings.defaultImages.label": "Default Saved Images",
      "dashboard.settings.defaultImages.help": "Default image cap for each batch.",
      "dashboard.settings.archiveLimit.label": "Archive Limit",
      "dashboard.settings.archiveLimit.help":
        "Archive completed tasks for stopped batches to reduce homepage query load.",
      "dashboard.settings.save": "Save Settings",
      "dashboard.settings.refresh": "Refresh Dashboard",
      "dashboard.settings.archiveDone": "Archive Completed Tasks",
      "dashboard.sections.newBatch.eyebrow": "New Batch",
      "dashboard.sections.newBatch.title": "Create Batch",
      "dashboard.sections.newBatch.note":
        "Provide a seed pin and rule set, then push it into the task queue immediately.",
      "dashboard.form.name.label": "Batch Name",
      "dashboard.form.name.placeholder": "For example: City Art 01",
      "dashboard.form.seed.label": "Seed Link",
      "dashboard.form.threshold.label": "Heart Threshold",
      "dashboard.form.maxImages.label": "Max Saved Images",
      "dashboard.form.maxDepth.label": "Max Depth",
      "dashboard.form.submit": "Create Batch",
      "dashboard.sections.library.eyebrow": "Live Library",
      "dashboard.sections.library.title": "Batch Wall",
      "dashboard.sections.library.note":
        "Watch covers, discovered links, scanned counts, and save progress in real time.",
      "dashboard.empty": "No batches yet. Create one to get started.",
      "dashboard.batch.coverPlaceholder": "Waiting for cover",
      "dashboard.batch.topHearts": "Top {count}",
      "dashboard.batch.progress": "{saved} / {max} saved",
      "dashboard.batch.start": "Start Batch",
      "dashboard.batch.stop": "Stop Batch",
      "dashboard.batch.open": "Open Detail",
      "dashboard.batch.card.running": "Running",
      "dashboard.batch.card.stopped": "Stopped",
      "dashboard.batch.stat.topHearts": "Top Hearts",
      "dashboard.batch.stat.discovered": "Discovered",
      "dashboard.batch.stat.scanned": "Scanned",
      "dashboard.batch.stat.depth": "Depth",
      "dashboard.batch.stat.pending": "Pending",
      "dashboard.batch.stat.savedRate": "Saved Rate",
      "dashboard.notice.settingsSaved": "Settings updated.",
      "dashboard.notice.refreshed": "Dashboard refreshed.",
      "dashboard.notice.archiveDone": "Archived {count} completed tasks.",
      "dashboard.notice.globalStarted": "Global tasks started.",
      "dashboard.notice.globalStopped": "Global tasks stopped.",
      "dashboard.notice.batchStarted": "Batch started.",
      "dashboard.notice.batchStopped": "Batch stopped.",
      "dashboard.notice.batchCreated": "Batch created.",
      "stats.totalPins": "Total Links",
      "stats.downloadedPins": "Downloaded",
      "stats.pendingTasks": "Pending",
      "stats.doingTasks": "Processing",
      "stats.errorTasks": "Failed",
      "stats.doneTasks": "Done",

      "detail.title": "PinPulse | Batch Detail",
      "detail.pageTitle": "PinPulse | {name}",
      "detail.back": "Back to Overview",
      "detail.hero.eyebrow": "Batch Detail",
      "detail.running": "Running",
      "detail.stopped": "Stopped",
      "detail.start": "Start",
      "detail.stop": "Stop",
      "detail.delete": "Delete",
      "detail.refresh": "Refresh",
      "detail.sections.filters.eyebrow": "Filters",
      "detail.sections.filters.title": "Browse and Rules",
      "detail.sections.filters.note":
        "Switch view mode, update thresholds, and apply depth changes to this batch immediately.",
      "detail.filters.viewMode.label": "View Mode",
      "detail.filters.viewMode.included": "Saved Only (wall)",
      "detail.filters.viewMode.all": "All Links (debug)",
      "detail.filters.limit.label": "Load Count",
      "detail.filters.threshold.label": "Threshold",
      "detail.filters.maxDepth.label": "Max Depth",
      "detail.filters.save": "Update Rules",
      "detail.sections.results.eyebrow": "Pinned Results",
      "detail.sections.results.title": "Image Wall",
      "detail.sections.results.note":
        "Keep crawling links, evaluate heart counts, and retain qualified assets locally.",
      "detail.empty": "No data yet.",
      "detail.pin.placeholder": "Link only",
      "detail.pin.link": "View Original Link",
      "detail.pin.locate": "Locate",
      "detail.pin.hearts": "{count} hearts",
      "detail.pin.level": "Level {level}",
      "detail.pin.levelUnknown": "Level -",
      "detail.pin.status.included": "Threshold passed and included in saved results.",
      "detail.pin.status.capped":
        "Threshold passed, but the batch image limit is already full. Link kept only.",
      "detail.pin.status.linkOnly": "Link only (threshold not reached).",
      "detail.pin.checkedAt": "Checked at: {value}",
      "detail.notice.started": "Batch started.",
      "detail.notice.stopped": "Batch stopped.",
      "detail.notice.refreshed": "Batch panel refreshed.",
      "detail.notice.rulesUpdated": "Batch rules updated.",
      "detail.notice.imageLocated": "Image location opened.",
      "detail.notice.imageUnavailable": "No local image or image source is available.",
      "detail.notice.imageLocateFailed": "Failed to locate image.",
      "detail.confirm.delete":
        "Delete this batch? This removes batch metadata and downloaded files unique to this batch. Shared link records stay intact.",
      "detail.error.invalidBatchId": "Invalid batch id.",
      "detail.error.thresholdMin": "Threshold must be >= 1.",
      "detail.error.maxDepthMin": "Max depth must be >= 1.",
      "detail.meta":
        "Seed: {seed} | Threshold: {threshold} | Max Depth: {maxDepth} | Top Hearts: {topHearts} | Saved: {saved}/{maxImages} | Discovered: {discovered} | Scanned: {scanned} | Pending: {pending}",

      "common.window.proxy": "Proxy Settings",
      "common.window.minimize": "Minimize",
      "common.window.close": "Close",
      "common.batchFallback": "批次 #{id}",
      "common.pinFallback": "Pin {id}",
      "common.language.label": "Language",
      "common.language.switcher": "Language selector",
      "common.notice.externalOpenFailed": "Unable to open external link.",
      "common.notice.windowMinimizeFailed": "Window minimize failed.",
      "common.notice.windowCloseFailed": "Window close failed.",
      "common.proxy.openFailed": "Proxy settings failed to open.",
      "common.proxy.dialog.eyebrow": "Network",
      "common.proxy.dialog.title": "Proxy Settings",
      "common.proxy.mode.label": "Mode",
      "common.proxy.mode.direct": "Direct",
      "common.proxy.mode.socks5": "SOCKS5",
      "common.proxy.mode.http": "HTTP",
      "common.proxy.mode.help": "Leave Direct selected to use the local connection.",
      "common.proxy.host.label": "Host",
      "common.proxy.port.label": "Port",
      "common.proxy.username.label": "Username",
      "common.proxy.password.label": "Password",
      "common.proxy.username.placeholder": "Optional",
      "common.proxy.password.placeholder": "Optional",
      "common.proxy.cancel": "Cancel",
      "common.proxy.save": "Save Proxy",
      "common.proxy.hostRequired": "Proxy host is required.",
      "common.proxy.portInvalid": "Proxy port must be between 1 and 65535.",
      "common.proxy.updated": "Proxy settings updated.",
      "common.proxy.disabled": "Proxy disabled.",
      "common.proxy.updateFailed": "Proxy settings update failed.",
      "common.proxy.close": "Close",

      "startup.title": "PinPulse Starting",
      "startup.eyebrow": "Desktop Workspace",
      "startup.copy":
        "Initializing the local database, scheduler, and embedded interface before entering the Pinterest heat collection workspace.",
      "startup.status.booting": "Waking up the workspace...",
      "startup.status.ready": "Interface is ready. Opening workspace...",
      "startup.status.connecting": "Connecting to internal services...",
      "startup.status.slower": "First screen is slower than usual, still loading...",
      "startup.badge": "Local-first",
    },
    "zh-CN": {
      "dashboard.title": "PinPulse | Pinterest 桌面工作台",
      "dashboard.hero.eyebrow": "Pinterest 桌面工作台",
      "dashboard.hero.copy":
        "从一个 Pinterest Pin 出发，持续发现相关链接、收集心数，并把达标图片沉淀到本地工作区。",
      "dashboard.pill.localFirst": "本地优先",
      "dashboard.pill.sqlite": "SQLite",
      "dashboard.pill.assets": "已固定素材",
      "dashboard.global.running": "全局运行中",
      "dashboard.global.stopped": "全局已停止",
      "dashboard.global.start": "全部开始",
      "dashboard.global.stop": "全部停止",
      "dashboard.sections.settings.eyebrow": "工作区",
      "dashboard.sections.settings.title": "系统设置",
      "dashboard.sections.settings.note": "控制全局并发、默认图片保留数量与任务归档。",
      "dashboard.settings.concurrency.label": "并发工作数",
      "dashboard.settings.concurrency.help": "建议范围：1-8。",
      "dashboard.settings.defaultImages.label": "默认保存图片数",
      "dashboard.settings.defaultImages.help": "每个 batch 默认保存的图片上限。",
      "dashboard.settings.archiveLimit.label": "归档上限",
      "dashboard.settings.archiveLimit.help": "归档已停止 batch 的已完成任务，降低首页查询压力。",
      "dashboard.settings.save": "保存设置",
      "dashboard.settings.refresh": "刷新面板",
      "dashboard.settings.archiveDone": "归档已完成任务",
      "dashboard.sections.newBatch.eyebrow": "新建 Batch",
      "dashboard.sections.newBatch.title": "创建 Batch",
      "dashboard.sections.newBatch.note": "填写种子链接和规则后，立即推入任务队列。",
      "dashboard.form.name.label": "Batch 名称",
      "dashboard.form.name.placeholder": "例如：City Art 01",
      "dashboard.form.seed.label": "种子链接",
      "dashboard.form.threshold.label": "心数阈值",
      "dashboard.form.maxImages.label": "最大保存图片数",
      "dashboard.form.maxDepth.label": "最大层级",
      "dashboard.form.submit": "创建 Batch",
      "dashboard.sections.library.eyebrow": "实时图库",
      "dashboard.sections.library.title": "Batch 墙",
      "dashboard.sections.library.note": "实时查看封面、发现量、扫描量与保存进度。",
      "dashboard.empty": "还没有 batch，请先创建一个。",
      "dashboard.batch.coverPlaceholder": "等待封面",
      "dashboard.batch.topHearts": "最高 {count}",
      "dashboard.batch.progress": "已保存 {saved} / {max}",
      "dashboard.batch.start": "开始任务",
      "dashboard.batch.stop": "停止任务",
      "dashboard.batch.open": "进入详情",
      "dashboard.batch.card.running": "运行中",
      "dashboard.batch.card.stopped": "已停止",
      "dashboard.batch.stat.topHearts": "最高心数",
      "dashboard.batch.stat.discovered": "发现",
      "dashboard.batch.stat.scanned": "已扫描",
      "dashboard.batch.stat.depth": "层级",
      "dashboard.batch.stat.pending": "待处理",
      "dashboard.batch.stat.savedRate": "保存率",
      "dashboard.notice.settingsSaved": "设置已更新。",
      "dashboard.notice.refreshed": "面板已刷新。",
      "dashboard.notice.archiveDone": "已归档 {count} 条完成任务。",
      "dashboard.notice.globalStarted": "全局任务已启动。",
      "dashboard.notice.globalStopped": "全局任务已停止。",
      "dashboard.notice.batchStarted": "Batch 已开始。",
      "dashboard.notice.batchStopped": "Batch 已停止。",
      "dashboard.notice.batchCreated": "新 batch 已创建。",
      "stats.totalPins": "总链接",
      "stats.downloadedPins": "已下载",
      "stats.pendingTasks": "待处理",
      "stats.doingTasks": "处理中",
      "stats.errorTasks": "失败",
      "stats.doneTasks": "已完成",

      "detail.title": "PinPulse | Batch 详情",
      "detail.pageTitle": "PinPulse | {name}",
      "detail.back": "返回总览",
      "detail.hero.eyebrow": "Batch 详情",
      "detail.running": "运行中",
      "detail.stopped": "已停止",
      "detail.start": "开始",
      "detail.stop": "停止",
      "detail.delete": "删除",
      "detail.refresh": "刷新",
      "detail.sections.filters.eyebrow": "筛选",
      "detail.sections.filters.title": "浏览与规则",
      "detail.sections.filters.note": "切换显示模式，调整阈值和层级，并立即作用于当前 batch。",
      "detail.filters.viewMode.label": "显示模式",
      "detail.filters.viewMode.included": "仅已保存（图片墙）",
      "detail.filters.viewMode.all": "全部链接（调试）",
      "detail.filters.limit.label": "加载数量",
      "detail.filters.threshold.label": "阈值",
      "detail.filters.maxDepth.label": "最大层级",
      "detail.filters.save": "更新规则",
      "detail.sections.results.eyebrow": "已收集结果",
      "detail.sections.results.title": "图片墙",
      "detail.sections.results.note": "持续抓取链接、判断心数，并把达标素材沉淀到本地。",
      "detail.empty": "当前没有数据。",
      "detail.pin.placeholder": "仅保留链接",
      "detail.pin.link": "查看原链接",
      "detail.pin.locate": "定位",
      "detail.pin.hearts": "{count} hearts",
      "detail.pin.level": "层级 {level}",
      "detail.pin.levelUnknown": "层级 -",
      "detail.pin.status.included": "达到阈值，已纳入保存。",
      "detail.pin.status.capped": "达到阈值，但 batch 图片上限已满，仅保留链接。",
      "detail.pin.status.linkOnly": "仅链接（未达到阈值）。",
      "detail.pin.checkedAt": "检查时间：{value}",
      "detail.notice.started": "Batch 已开始。",
      "detail.notice.stopped": "Batch 已停止。",
      "detail.notice.refreshed": "Batch 面板已刷新。",
      "detail.notice.rulesUpdated": "Batch 规则已更新。",
      "detail.notice.imageLocated": "已打开图片位置。",
      "detail.notice.imageUnavailable": "没有可定位的图片或图片链接。",
      "detail.notice.imageLocateFailed": "定位图片失败。",
      "detail.confirm.delete": "确认删除这个 Batch？会移除该 Batch 信息和仅属于该 Batch 的已下载文件；共享链接记录会保留。",
      "detail.error.invalidBatchId": "无效的 batch id。",
      "detail.error.thresholdMin": "阈值必须 >= 1。",
      "detail.error.maxDepthMin": "最大层级必须 >= 1。",
      "detail.meta":
        "Seed：{seed} | 阈值：{threshold} | 最大层级：{maxDepth} | 最高心数：{topHearts} | 已保存：{saved}/{maxImages} | 已发现：{discovered} | 已扫描：{scanned} | 待处理：{pending}",

      "common.window.proxy": "代理设置",
      "common.window.minimize": "最小化",
      "common.window.close": "关闭",
      "common.batchFallback": "Batch #{id}",
      "common.pinFallback": "Pin {id}",
      "common.language.label": "语言",
      "common.language.switcher": "语言切换",
      "common.notice.externalOpenFailed": "无法打开外部链接。",
      "common.notice.windowMinimizeFailed": "窗口最小化失败。",
      "common.notice.windowCloseFailed": "窗口关闭失败。",
      "common.proxy.openFailed": "无法打开代理设置。",
      "common.proxy.dialog.eyebrow": "网络",
      "common.proxy.dialog.title": "代理设置",
      "common.proxy.mode.label": "模式",
      "common.proxy.mode.direct": "直连",
      "common.proxy.mode.socks5": "SOCKS5",
      "common.proxy.mode.http": "HTTP",
      "common.proxy.mode.help": "保持直连即可使用本机网络。",
      "common.proxy.host.label": "地址",
      "common.proxy.port.label": "端口",
      "common.proxy.username.label": "用户名",
      "common.proxy.password.label": "密码",
      "common.proxy.username.placeholder": "可选",
      "common.proxy.password.placeholder": "可选",
      "common.proxy.cancel": "取消",
      "common.proxy.save": "保存代理",
      "common.proxy.hostRequired": "代理地址不能为空。",
      "common.proxy.portInvalid": "代理端口必须在 1 到 65535 之间。",
      "common.proxy.updated": "代理设置已更新。",
      "common.proxy.disabled": "已关闭代理。",
      "common.proxy.updateFailed": "代理设置更新失败。",
      "common.proxy.close": "关闭",

      "startup.title": "PinPulse 启动中",
      "startup.eyebrow": "桌面工作区",
      "startup.copy": "正在初始化本地数据库、调度器与嵌入式界面，准备进入 Pinterest 热度采集工作台。",
      "startup.status.booting": "正在唤醒工作区...",
      "startup.status.ready": "界面已就绪，正在展开工作台...",
      "startup.status.connecting": "正在连接内部服务...",
      "startup.status.slower": "首屏加载比平时稍慢，仍在继续...",
      "startup.badge": "Local-first",
    },
  };

  function resolveLocale(raw) {
    const value = String(raw || "").trim();
    if (messages[value]) {
      return value;
    }
    return DEFAULT_LOCALE;
  }

  function readStoredLocale() {
    try {
      return resolveLocale(window.localStorage.getItem(STORAGE_KEY));
    } catch (_) {
      return DEFAULT_LOCALE;
    }
  }

  let currentLocale = readStoredLocale();

  function writeStoredLocale(locale) {
    try {
      window.localStorage.setItem(STORAGE_KEY, locale);
    } catch (_) {
      // Ignore storage failures.
    }
  }

  function lookup(locale, key) {
    return messages[locale]?.[key];
  }

  function interpolate(template, params = {}) {
    return String(template).replace(/\{(\w+)\}/g, (_, key) => {
      const value = params[key];
      return value === undefined || value === null ? "" : String(value);
    });
  }

  function t(key, params) {
    const template = lookup(currentLocale, key) ?? lookup(DEFAULT_LOCALE, key) ?? key;
    return interpolate(template, params);
  }

  function withRoot(root) {
    if (!root) {
      return [];
    }

    const items = [];
    const canMatch = typeof root.matches === "function";
    if (
      canMatch &&
      root.matches("[data-i18n], [data-i18n-placeholder], [data-i18n-title], [data-i18n-aria-label]")
    ) {
      items.push(root);
    }
    if (typeof root.querySelectorAll === "function") {
      items.push(
        ...root.querySelectorAll(
          "[data-i18n], [data-i18n-placeholder], [data-i18n-title], [data-i18n-aria-label]"
        )
      );
    }
    return items;
  }

  function translateTree(root = document) {
    const nodes = withRoot(root);
    nodes.forEach((node) => {
      const textKey = node.getAttribute("data-i18n");
      if (textKey) {
        node.textContent = t(textKey);
      }

      const placeholderKey = node.getAttribute("data-i18n-placeholder");
      if (placeholderKey) {
        node.setAttribute("placeholder", t(placeholderKey));
      }

      const titleKey = node.getAttribute("data-i18n-title");
      if (titleKey) {
        node.setAttribute("title", t(titleKey));
      }

      const ariaKey = node.getAttribute("data-i18n-aria-label");
      if (ariaKey) {
        node.setAttribute("aria-label", t(ariaKey));
      }
    });
  }

  function applyDocumentLanguage() {
    document.documentElement.lang = currentLocale;
  }

  function getLocale() {
    return currentLocale;
  }

  function setLocale(locale) {
    const nextLocale = resolveLocale(locale);
    if (nextLocale === currentLocale) {
      return currentLocale;
    }

    currentLocale = nextLocale;
    writeStoredLocale(nextLocale);
    applyDocumentLanguage();
    document.dispatchEvent(
      new CustomEvent("pinpulse:locale-changed", {
        detail: { locale: nextLocale },
      })
    );
    return currentLocale;
  }

  function getSupportedLocales() {
    return Object.keys(messages).map((code) => ({
      code,
      label: LABELS[code] || code,
    }));
  }

  function formatNumber(value) {
    const num = Number(value || 0);
    if (!Number.isFinite(num)) {
      return "0";
    }
    return new Intl.NumberFormat(currentLocale).format(num);
  }

  function formatDate(value) {
    if (!value) {
      return "";
    }
    const date = new Date(value);
    if (Number.isNaN(date.valueOf())) {
      return String(value);
    }
    return date.toLocaleString(currentLocale);
  }

  applyDocumentLanguage();

  window.PinPulseI18n = {
    defaultLocale: DEFAULT_LOCALE,
    getLocale,
    setLocale,
    getSupportedLocales,
    translateTree,
    formatDate,
    formatNumber,
    t,
  };
})();
