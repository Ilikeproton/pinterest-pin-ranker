(function () {
  const I18n = window.PinPulseI18n;
  const noticeRootId = "noticeStack";
  const proxyDialogId = "proxySettingsDialog";
  const topRightControlsId = "desktopWindowControls";

  function ensureNoticeRoot() {
    let root = document.getElementById(noticeRootId);
    if (!root) {
      root = document.createElement("div");
      root.id = noticeRootId;
      root.className = "notice-stack";
      document.body.appendChild(root);
    }
    return root;
  }

  function showNotice(message, kind = "info") {
    const root = ensureNoticeRoot();
    const notice = document.createElement("div");
    notice.className = `notice notice--${kind}`;
    notice.textContent = message;
    root.appendChild(notice);
    window.setTimeout(() => {
      notice.remove();
      if (!root.childElementCount) {
        root.remove();
      }
    }, 3200);
  }

  async function request(url, options = {}) {
    const headers = new Headers(options.headers || {});
    if (options.body && !headers.has("Content-Type")) {
      headers.set("Content-Type", "application/json");
    }

    const response = await fetch(url, {
      ...options,
      headers,
    });
    const text = await response.text();
    let data = {};
    if (text) {
      try {
        data = JSON.parse(text);
      } catch (_) {
        data = { raw: text };
      }
    }
    if (!response.ok) {
      throw new Error(data.error || data.raw || `HTTP ${response.status}`);
    }
    return data;
  }

  function shortUrl(url) {
    if (!url) return "";
    if (url.length <= 58) return url;
    return `${url.slice(0, 55)}...`;
  }

  function toAbsoluteURL(rawURL) {
    const value = String(rawURL || "").trim();
    if (!value) {
      return "";
    }
    return new URL(value, window.location.href).toString();
  }

  async function locateImage({ sourcePath = "", sourceURL = "" } = {}) {
    const localPath = String(sourcePath || "").trim();
    const absoluteURL = toAbsoluteURL(sourceURL);
    if (!localPath && !absoluteURL) {
      throw new Error(t("detail.notice.imageUnavailable"));
    }

    if (typeof window.pinpulseLocateImage === "function") {
      const locatedPath = await window.pinpulseLocateImage(localPath, absoluteURL);
      return locatedPath || "";
    }

    if (absoluteURL) {
      await openExternal(absoluteURL);
      return absoluteURL;
    }

    throw new Error(t("detail.notice.imageUnavailable"));
  }

  function prettyDate(value) {
    return I18n.formatDate(value);
  }

  function formatNumber(value) {
    return I18n.formatNumber(value);
  }

  function t(key, params) {
    return I18n.t(key, params);
  }

  function translateTree(root = document) {
    I18n.translateTree(root);
  }

  async function openExternal(url) {
    const target = String(url || "").trim();
    if (!target) return;
    if (typeof window.pinpulseOpenExternal === "function") {
      await window.pinpulseOpenExternal(target);
      return;
    }
    window.open(target, "_blank", "noopener");
  }

  function bindExternalLinks(root = document) {
    root.addEventListener("click", (event) => {
      const anchor = event.target.closest('a[data-external], a[target="_blank"]');
      if (!anchor) return;

      const href = anchor.getAttribute("href");
      if (!href) return;

      let target;
      try {
        target = new URL(href, window.location.href);
      } catch (_) {
        return;
      }
      if (target.origin === window.location.origin && !anchor.hasAttribute("data-external")) {
        return;
      }

      event.preventDefault();
      openExternal(target.toString()).catch(() => {
        showNotice(t("common.notice.externalOpenFailed"), "error");
      });
    });
  }

  function disableContextMenu(root = document) {
    root.addEventListener("contextmenu", (event) => {
      event.preventDefault();
    });
  }

  function signalDesktopReady() {
    if (typeof window.pinpulseDesktopReady !== "function") {
      return;
    }
    window.pinpulseDesktopReady().catch(() => {});
  }

  function closeProxyDialog() {
    const dialog = document.getElementById(proxyDialogId);
    if (!dialog) {
      return;
    }
    dialog.classList.add("is-hidden");
    dialog.setAttribute("aria-hidden", "true");
    document.body.classList.remove("proxy-dialog-open");
  }

  function syncProxyFieldState(dialog) {
    const form = dialog?.querySelector("#proxySettingsForm");
    if (!form) {
      return;
    }

    const isDirect = !String(form.elements.namedItem("proxy_type")?.value || "").trim();
    ["proxy_host", "proxy_port", "proxy_username", "proxy_password"].forEach((name) => {
      const field = form.elements.namedItem(name);
      if (field) {
        field.disabled = isDirect;
      }
    });
  }

  function ensureProxyDialog() {
    let dialog = document.getElementById(proxyDialogId);
    if (!dialog) {
      dialog = document.createElement("div");
      dialog.id = proxyDialogId;
      dialog.className = "proxy-dialog is-hidden";
      dialog.setAttribute("aria-hidden", "true");
      dialog.innerHTML = `
        <div class="proxy-dialog__backdrop" data-close-proxy-dialog></div>
        <div class="proxy-dialog__panel" role="dialog" aria-modal="true" aria-labelledby="proxyDialogTitle">
          <div class="proxy-dialog__head">
            <div>
              <p class="proxy-dialog__eyebrow" data-i18n="common.proxy.dialog.eyebrow">Network</p>
              <h2 id="proxyDialogTitle" data-i18n="common.proxy.dialog.title">Proxy Settings</h2>
            </div>
            <button
              class="proxy-dialog__close"
              type="button"
              data-close-proxy-dialog
              data-i18n-aria-label="common.proxy.close"
              data-i18n-title="common.proxy.close"
              aria-label="Close"
              title="Close"
            >x</button>
          </div>
          <form class="proxy-form" id="proxySettingsForm">
            <label class="field field--wide">
              <span class="field-label" data-i18n="common.proxy.mode.label">Mode</span>
              <select class="input" name="proxy_type">
                <option value="" data-i18n="common.proxy.mode.direct">Direct</option>
                <option value="socks5" data-i18n="common.proxy.mode.socks5">SOCKS5</option>
                <option value="http" data-i18n="common.proxy.mode.http">HTTP</option>
              </select>
              <span class="field-help" data-i18n="common.proxy.mode.help">Leave Direct selected to use the local connection.</span>
            </label>

            <label class="field">
              <span class="field-label" data-i18n="common.proxy.host.label">Host</span>
              <input class="input" name="proxy_host" type="text" autocomplete="off" placeholder="127.0.0.1" />
            </label>

            <label class="field">
              <span class="field-label" data-i18n="common.proxy.port.label">Port</span>
              <input class="input" name="proxy_port" type="number" min="1" max="65535" placeholder="1902" />
            </label>

            <label class="field">
              <span class="field-label" data-i18n="common.proxy.username.label">Username</span>
              <input
                class="input"
                name="proxy_username"
                type="text"
                autocomplete="off"
                data-i18n-placeholder="common.proxy.username.placeholder"
                placeholder="Optional"
              />
            </label>

            <label class="field">
              <span class="field-label" data-i18n="common.proxy.password.label">Password</span>
              <input
                class="input"
                name="proxy_password"
                type="password"
                autocomplete="new-password"
                data-i18n-placeholder="common.proxy.password.placeholder"
                placeholder="Optional"
              />
            </label>

            <div class="proxy-form__actions">
              <button class="button button--ghost" type="button" data-close-proxy-dialog data-i18n="common.proxy.cancel">Cancel</button>
              <button class="button button--accent" type="submit" data-i18n="common.proxy.save">Save Proxy</button>
            </div>
          </form>
        </div>
      `;

      dialog.addEventListener("click", (event) => {
        if (event.target.closest("[data-close-proxy-dialog]")) {
          closeProxyDialog();
        }
      });

      const form = dialog.querySelector("#proxySettingsForm");
      const typeInput = form?.elements?.namedItem("proxy_type");
      typeInput?.addEventListener("change", () => {
        syncProxyFieldState(dialog);
      });

      form?.addEventListener("submit", async (event) => {
        event.preventDefault();

        const type = String(form.elements.namedItem("proxy_type")?.value || "").trim();
        const host = String(form.elements.namedItem("proxy_host")?.value || "").trim();
        const portRaw = Number(form.elements.namedItem("proxy_port")?.value || 0);
        const username = String(form.elements.namedItem("proxy_username")?.value || "").trim();
        const password = String(form.elements.namedItem("proxy_password")?.value || "");

        let payload;
        if (!type) {
          payload = {
            proxy_type: "",
            proxy_host: "",
            proxy_port: 0,
            proxy_username: "",
            proxy_password: "",
          };
        } else {
          if (!host) {
            showNotice(t("common.proxy.hostRequired"), "error");
            return;
          }
          if (!Number.isFinite(portRaw) || portRaw < 1 || portRaw > 65535) {
            showNotice(t("common.proxy.portInvalid"), "error");
            return;
          }
          payload = {
            proxy_type: type,
            proxy_host: host,
            proxy_port: portRaw,
            proxy_username: username,
            proxy_password: password,
          };
        }

        try {
          await request("/api/settings", {
            method: "PUT",
            body: JSON.stringify(payload),
          });
          closeProxyDialog();
          showNotice(type ? t("common.proxy.updated") : t("common.proxy.disabled"), "success");
          document.dispatchEvent(new CustomEvent("pinpulse:settings-updated"));
        } catch (_) {
          showNotice(t("common.proxy.updateFailed"), "error");
        }
      });

      document.addEventListener("keydown", (event) => {
        if (event.key === "Escape" && !dialog.classList.contains("is-hidden")) {
          closeProxyDialog();
        }
      });

      document.body.appendChild(dialog);
    }

    translateTree(dialog);
    syncProxyFieldState(dialog);
    return dialog;
  }

  async function openProxyDialog() {
    const dialog = ensureProxyDialog();
    const settings = await request("/api/settings");
    const form = dialog.querySelector("#proxySettingsForm");
    if (!form) {
      return;
    }

    form.elements.namedItem("proxy_type").value = settings.proxy_type || "";
    form.elements.namedItem("proxy_host").value = settings.proxy_host || "";
    form.elements.namedItem("proxy_port").value = settings.proxy_port || "";
    form.elements.namedItem("proxy_username").value = settings.proxy_username || "";
    form.elements.namedItem("proxy_password").value = settings.proxy_password || "";
    syncProxyFieldState(dialog);

    dialog.classList.remove("is-hidden");
    dialog.setAttribute("aria-hidden", "false");
    document.body.classList.add("proxy-dialog-open");
    form.elements.namedItem("proxy_type").focus();
  }

  function buildLocaleSelector(root) {
    const wrap = document.createElement("label");
    wrap.className = "top-control-select";
    wrap.setAttribute("aria-label", t("common.language.switcher"));
    wrap.title = t("common.language.label");

    const icon = document.createElement("span");
    icon.className = "top-control-select__icon";
    icon.textContent = "A";

    const select = document.createElement("select");
    select.className = "top-control-select__input";
    select.setAttribute("aria-label", t("common.language.switcher"));

    I18n.getSupportedLocales().forEach((item) => {
      const option = document.createElement("option");
      option.value = item.code;
      option.textContent = item.label;
      select.appendChild(option);
    });
    select.value = I18n.getLocale();
    select.addEventListener("change", (event) => {
      I18n.setLocale(event.target.value);
    });

    wrap.append(icon, select);
    root.appendChild(wrap);
  }

  function buildIconButton({ className = "", titleKey, text = "", html = "", onClick }) {
    const button = document.createElement("button");
    button.className = `window-control ${className}`.trim();
    button.type = "button";
    button.title = t(titleKey);
    button.setAttribute("aria-label", t(titleKey));
    if (html) {
      button.innerHTML = html;
    } else {
      button.textContent = text;
    }
    button.addEventListener("click", onClick);
    return button;
  }

  function ensureTopRightControls() {
    let root = document.getElementById(topRightControlsId);
    if (!root) {
      root = document.createElement("div");
      root.id = topRightControlsId;
      root.className = "window-controls";
      document.body.appendChild(root);
    }

    root.replaceChildren();
    document.body.classList.add("has-top-controls");

    buildLocaleSelector(root);

    root.appendChild(
      buildIconButton({
        className: "window-control--proxy",
        titleKey: "common.window.proxy",
        html:
          '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M19.14 12.94a7.43 7.43 0 0 0 .05-.94 7.43 7.43 0 0 0-.05-.94l2.03-1.58a.5.5 0 0 0 .12-.64l-1.92-3.32a.5.5 0 0 0-.6-.22l-2.39.96a7.2 7.2 0 0 0-1.63-.94l-.36-2.54a.5.5 0 0 0-.5-.42h-3.84a.5.5 0 0 0-.49.42l-.37 2.54c-.58.23-1.12.54-1.63.94l-2.39-.96a.5.5 0 0 0-.6.22l-1.92 3.32a.5.5 0 0 0 .12.64l2.03 1.58a7.43 7.43 0 0 0-.05.94c0 .32.02.63.05.94l-2.03 1.58a.5.5 0 0 0-.12.64l1.92 3.32a.5.5 0 0 0 .6.22l2.39-.96c.5.4 1.05.72 1.63.94l.37 2.54a.5.5 0 0 0 .49.42h3.84a.5.5 0 0 0 .5-.42l.36-2.54c.58-.23 1.13-.54 1.63-.94l2.39.96a.5.5 0 0 0 .6-.22l1.92-3.32a.5.5 0 0 0-.12-.64l-2.03-1.58ZM12 15.5A3.5 3.5 0 1 1 12 8a3.5 3.5 0 0 1 0 7.5Z"/></svg>',
        onClick: () => {
          openProxyDialog().catch(() => {
            showNotice(t("common.proxy.openFailed"), "error");
          });
        },
      })
    );

    if (typeof window.pinpulseWindowMinimize === "function") {
      root.appendChild(
        buildIconButton({
          titleKey: "common.window.minimize",
          text: "-",
          onClick: () => {
            window.pinpulseWindowMinimize().catch(() => {
              showNotice(t("common.notice.windowMinimizeFailed"), "error");
            });
          },
        })
      );
    }

    if (typeof window.pinpulseWindowClose === "function") {
      root.appendChild(
        buildIconButton({
          className: "window-control--close",
          titleKey: "common.window.close",
          text: "x",
          onClick: () => {
            window.pinpulseWindowClose().catch(() => {
              showNotice(t("common.notice.windowCloseFailed"), "error");
            });
          },
        })
      );
    }

    ensureProxyDialog();
    return root;
  }

  window.PinPulseUI = {
    locateImage,
    openExternal,
    openProxyDialog,
    prettyDate,
    formatNumber,
    request,
    shortUrl,
    showNotice,
    signalDesktopReady,
    t,
    translateTree,
  };

  document.addEventListener(
    "DOMContentLoaded",
    () => {
      ensureTopRightControls();
      translateTree(document);
      disableContextMenu(document);
      bindExternalLinks(document);
    },
    { once: true }
  );

  document.addEventListener("pinpulse:locale-changed", () => {
    ensureTopRightControls();
    translateTree(document);
  });
})();
