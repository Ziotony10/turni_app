(function () {
  const STYLE_ID = "app-notifications-style";
  const ROOT_CLASS = "app-notifications-menu";
  let notifications = [];

  function injectStyle() {
    if (document.getElementById(STYLE_ID)) return;
    const style = document.createElement("style");
    style.id = STYLE_ID;
    style.textContent = `
      .${ROOT_CLASS}{position:relative;margin-left:4px}
      .notif-bell{position:relative;cursor:pointer;padding:6px;border-radius:8px;background:var(--card2);border:1px solid var(--border);color:var(--muted);display:flex;align-items:center;justify-content:center;height:32px;width:32px;transition:all .15s;font-size:1rem;line-height:1}
      .notif-bell:hover{background:#2d3f55;color:var(--text)}
      .notif-badge{position:absolute;top:-4px;right:-4px;background:#ef4444;color:white;font-size:.65rem;font-weight:800;min-width:16px;height:16px;border-radius:999px;display:none;align-items:center;justify-content:center;border:2px solid var(--card)}
      .notif-badge.show{display:flex}
      .notif-dropdown{display:none;position:absolute;right:0;top:calc(100% + 6px);background:var(--card);border:1px solid var(--border);border-radius:12px;width:min(320px,calc(100vw - 24px));max-height:450px;z-index:250;box-shadow:0 12px 32px rgba(0,0,0,.5);overflow:hidden;flex-direction:column}
      .notif-dropdown.open{display:flex}
      .notif-header{padding:12px 16px;background:var(--card2);border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;gap:12px}
      .notif-header h4{font-size:.85rem;font-weight:700;margin:0}
      .notif-mark-read{font-size:.72rem;color:var(--accent);cursor:pointer;white-space:nowrap}
      .notif-list{overflow-y:auto;flex:1}
      .notif-item{padding:12px 16px;border-bottom:1px solid var(--border);cursor:pointer;transition:background .1s;position:relative}
      .notif-item:hover{background:rgba(255,255,255,.03)}
      .notif-item.unread{background:rgba(59,130,246,.05)}
      .notif-item.unread::before{content:'';position:absolute;left:6px;top:50%;transform:translateY(-50%);width:6px;height:6px;border-radius:50%;background:var(--accent)}
      .notif-title{font-size:.8rem;font-weight:700;color:var(--text);margin-bottom:2px}
      .notif-msg{font-size:.74rem;color:var(--muted);line-height:1.4}
      .notif-time{font-size:.65rem;color:var(--muted);margin-top:6px}
      .notif-empty{padding:30px;text-align:center;color:var(--muted);font-size:.8rem}
      .feedback-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.62);z-index:360;align-items:center;justify-content:center;padding:16px}
      .feedback-overlay.open{display:flex}
      .feedback-modal{width:min(460px,100%);background:var(--card);border:1px solid var(--border);border-radius:14px;box-shadow:0 18px 46px rgba(0,0,0,.55);overflow:hidden}
      .feedback-head{padding:14px 18px;background:var(--card2);border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;gap:12px}
      .feedback-head h3{font-size:.95rem;margin:0;color:var(--text)}
      .feedback-close{background:none;border:none;color:var(--muted);font-size:1.15rem;cursor:pointer}
      .feedback-body{padding:18px}
      .feedback-body p{font-size:.82rem;color:var(--muted);line-height:1.45;margin:0 0 12px}
      .feedback-body textarea{width:100%;min-height:132px;resize:vertical;background:var(--card2);border:1px solid var(--border);border-radius:10px;color:var(--text);padding:11px 12px;font:inherit;font-size:.9rem;outline:none}
      .feedback-body textarea:focus{border-color:var(--accent);box-shadow:0 0 0 2px rgba(59,130,246,.16)}
      .feedback-actions{display:flex;justify-content:flex-end;gap:8px;padding:0 18px 18px}
      .feedback-btn{border:none;border-radius:8px;padding:8px 14px;font-size:.84rem;font-weight:700;cursor:pointer}
      .feedback-btn.secondary{background:var(--card2);border:1px solid var(--border);color:var(--muted)}
      .feedback-btn.primary{background:var(--accent);color:white}
      .feedback-msg{min-height:18px;margin-top:10px;font-size:.8rem;color:var(--muted)}
    `;
    document.head.appendChild(style);
  }

  function escapeHtml(value) {
    return String(value ?? "").replace(/[&<>"']/g, ch => ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;"
    }[ch]));
  }

  function formatNotificationTime(value) {
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return "";
    return d.toLocaleString("it-IT", {
      day: "2-digit",
      month: "2-digit",
      year: "numeric",
      hour: "2-digit",
      minute: "2-digit"
    });
  }

  function formatItalianDateText(value) {
    return String(value ?? "").replace(/\b(\d{4})-(\d{2})-(\d{2})\b/g, "$3-$2-$1");
  }

  function render() {
    const list = document.getElementById("notifList");
    const badge = document.getElementById("notifBadge");
    if (!list || !badge) return;

    const unread = notifications.filter(n => !n.is_read);
    badge.textContent = unread.length;
    badge.classList.toggle("show", unread.length > 0);

    if (notifications.length === 0) {
      list.innerHTML = '<div class="notif-empty">Nessuna notifica</div>';
      return;
    }

    list.innerHTML = notifications.map(n => `
      <div class="notif-item ${n.is_read ? "" : "unread"}" data-notification-id="${Number(n.id)}">
        <div class="notif-title">${escapeHtml(n.title || "Notifica")}</div>
        <div class="notif-msg">${escapeHtml(formatItalianDateText(n.message || ""))}</div>
        <div class="notif-time">${escapeHtml(formatNotificationTime(n.created_at))}</div>
      </div>
    `).join("");
  }

  async function loadNotifications() {
    notifications = await fetch("/api/team/notifications")
      .then(r => r.ok ? r.json() : [])
      .catch(() => []);
    render();
  }

  async function handleNotificationClick(id) {
    const item = notifications.find(n => Number(n.id) === Number(id));
    await fetch(`/api/team/notifications/${id}/read`, { method: "POST" });
    if (item && item.link) {
      window.location.href = item.link;
      return;
    }
    await loadNotifications();
  }

  function createWidget() {
    if (document.getElementById("notifBellBtn")) return false;
    const target = document.querySelector("header .header-controls");
    if (!target) return false;
    const wrapper = document.createElement("div");
    wrapper.className = ROOT_CLASS;
    wrapper.innerHTML = `
      <button type="button" class="notif-bell" id="notifBellBtn" aria-label="Notifiche">
        <span aria-hidden="true">&#128276;</span>
        <span class="notif-badge" id="notifBadge">0</span>
      </button>
      <div class="notif-dropdown" id="notifDropdown">
        <div class="notif-header">
          <h4>Notifiche</h4>
          <span class="notif-mark-read" id="notifMarkAllRead">Segna tutto come letto</span>
        </div>
        <div class="notif-list" id="notifList">
          <div class="notif-empty">Nessuna notifica</div>
        </div>
      </div>
    `;
    target.appendChild(wrapper);
    return true;
  }

  function bindEvents() {
    const bell = document.getElementById("notifBellBtn");
    const dropdown = document.getElementById("notifDropdown");
    const list = document.getElementById("notifList");
    const markAll = document.getElementById("notifMarkAllRead");
    if (!bell || !dropdown || !list) return;

    bell.addEventListener("click", event => {
      event.stopPropagation();
      dropdown.classList.toggle("open");
      if (dropdown.classList.contains("open")) loadNotifications();
    });
    dropdown.addEventListener("click", event => event.stopPropagation());
    list.addEventListener("click", event => {
      const item = event.target.closest(".notif-item");
      if (item) handleNotificationClick(item.dataset.notificationId);
    });
    if (markAll) {
      markAll.addEventListener("click", async () => {
        await fetch("/api/team/notifications/read-all", { method: "POST" });
        await loadNotifications();
      });
    }
    document.addEventListener("click", () => dropdown.classList.remove("open"));
  }

  function authedFetch(url, opts = {}) {
    const token = localStorage.getItem("token");
    const headers = { ...(opts.headers || {}) };
    if (token) headers.Authorization = `Bearer ${token}`;
    return fetch(url, { ...opts, headers });
  }

  function ensureUserMenuExtras() {
    const dropdown = document.getElementById("userDropdown");
    if (!dropdown || dropdown.dataset.commonExtras === "1") return;
    dropdown.dataset.commonExtras = "1";

    const firstSep = dropdown.querySelector(".ud-sep");
    const guide = document.createElement("div");
    guide.className = "ud-item";
    guide.textContent = "📘 Guida";
    guide.addEventListener("click", event => {
      event.stopPropagation();
      window.location.href = "/guida.html";
    });

    const feedback = document.createElement("div");
    feedback.className = "ud-item";
    feedback.textContent = "💬 Invia feedback";
    feedback.addEventListener("click", event => {
      event.stopPropagation();
      openFeedbackModal();
    });

    const sep = document.createElement("div");
    sep.className = "ud-sep";

    if (firstSep) {
      dropdown.insertBefore(guide, firstSep);
      dropdown.insertBefore(feedback, firstSep);
      dropdown.insertBefore(sep, firstSep);
    } else {
      dropdown.appendChild(guide);
      dropdown.appendChild(feedback);
    }
  }

  function ensureFeedbackModal() {
    if (document.getElementById("feedbackOverlay")) return;
    const overlay = document.createElement("div");
    overlay.className = "feedback-overlay";
    overlay.id = "feedbackOverlay";
    overlay.innerHTML = `
      <div class="feedback-modal" role="dialog" aria-modal="true" aria-labelledby="feedbackTitle">
        <div class="feedback-head">
          <h3 id="feedbackTitle">Invia feedback</h3>
          <button type="button" class="feedback-close" id="feedbackCloseBtn" aria-label="Chiudi">×</button>
        </div>
        <div class="feedback-body">
          <p>Scrivi un problema, un dubbio o una proposta di miglioramento. Gli admin la leggeranno dal pannello Admin.</p>
          <textarea id="feedbackText" maxlength="2000" placeholder="Esempio: nella pagina Team non capisco se la richiesta ferie e' stata inviata..."></textarea>
          <div class="feedback-msg" id="feedbackMsg"></div>
        </div>
        <div class="feedback-actions">
          <button type="button" class="feedback-btn secondary" id="feedbackCancelBtn">Annulla</button>
          <button type="button" class="feedback-btn primary" id="feedbackSendBtn">Invia</button>
        </div>
      </div>
    `;
    document.body.appendChild(overlay);
    overlay.addEventListener("click", event => {
      if (event.target === overlay) closeFeedbackModal();
    });
    overlay.querySelector("#feedbackCloseBtn")?.addEventListener("click", closeFeedbackModal);
    overlay.querySelector("#feedbackCancelBtn")?.addEventListener("click", closeFeedbackModal);
    overlay.querySelector("#feedbackSendBtn")?.addEventListener("click", submitFeedback);
  }

  function openFeedbackModal() {
    ensureFeedbackModal();
    document.getElementById("userDropdown")?.classList.remove("open");
    const overlay = document.getElementById("feedbackOverlay");
    const msg = document.getElementById("feedbackMsg");
    if (msg) msg.textContent = "";
    overlay?.classList.add("open");
    setTimeout(() => document.getElementById("feedbackText")?.focus(), 0);
  }

  function closeFeedbackModal() {
    document.getElementById("feedbackOverlay")?.classList.remove("open");
  }

  async function submitFeedback() {
    const textarea = document.getElementById("feedbackText");
    const msg = document.getElementById("feedbackMsg");
    const btn = document.getElementById("feedbackSendBtn");
    const value = (textarea?.value || "").trim();
    if (!value || value.length < 5) {
      if (msg) {
        msg.textContent = "Scrivi almeno qualche parola per descrivere la segnalazione.";
        msg.style.color = "#F59E0B";
      }
      return;
    }
    if (btn) btn.disabled = true;
    if (msg) {
      msg.textContent = "Invio in corso...";
      msg.style.color = "var(--muted)";
    }
    const res = await authedFetch("/api/feedback", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ messaggio: value, pagina: window.location.pathname })
    }).then(r => r.ok ? r.json() : r.json().then(x => Promise.reject(x))).catch(err => ({ ok: false, detail: err?.detail || "Errore durante l'invio" }));
    if (btn) btn.disabled = false;
    if (res.ok) {
      if (textarea) textarea.value = "";
      if (msg) {
        msg.textContent = "Feedback inviato. Grazie, lo troveranno gli admin.";
        msg.style.color = "#34D399";
      }
      setTimeout(closeFeedbackModal, 900);
    } else if (msg) {
      msg.textContent = res.detail || "Errore durante l'invio";
      msg.style.color = "#FCA5A5";
    }
  }

  document.addEventListener("DOMContentLoaded", () => {
    if (!localStorage.getItem("token")) return;
    injectStyle();
    ensureUserMenuExtras();
    ensureFeedbackModal();
    if (createWidget()) {
      bindEvents();
      loadNotifications();
      window.setInterval(loadNotifications, 60000);
    }
  });
})();
