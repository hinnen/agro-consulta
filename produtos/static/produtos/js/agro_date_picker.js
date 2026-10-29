/**
 * Calendário popup (mesmo padrão Nova saída) — reutilizável em qualquer tela.
 * window.AgroDatePicker.bind(root?, { accent, accentSoft })
 */
(function () {
  'use strict';

  const CAL_MESES = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho', 'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro'];
  const CAL_DOW = ['D', 'S', 'T', 'Q', 'Q', 'S', 'S'];

  let calPop = null;
  let calAnchor = null;
  let calView = null;
  let calAccent = '#059669';
  let calAccentSoft = '#ecfdf5';

  function todayISO() {
    const d = new Date();
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
  }

  function calEnsurePop() {
    if (calPop) return calPop;
    calPop = document.createElement('div');
    calPop.id = 'agro-cal-pop-shared';
    calPop.className = 'agro-ns-cal-pop hidden';
    calPop.setAttribute('role', 'dialog');
    calPop.setAttribute('aria-label', 'Escolher data');
    calPop.innerHTML = `
      <div class="agro-ns-cal-pop-head">
        <button type="button" class="agro-ns-cal-pop-nav" data-cal-nav="-1" aria-label="Mês anterior">‹</button>
        <div class="agro-ns-cal-pop-title" data-cal-title></div>
        <button type="button" class="agro-ns-cal-pop-nav" data-cal-nav="1" aria-label="Próximo mês">›</button>
      </div>
      <div class="agro-ns-cal-pop-dow">${CAL_DOW.map((d) => `<span>${d}</span>`).join('')}</div>
      <div class="agro-ns-cal-pop-grid" data-cal-grid></div>
      <div class="agro-ns-cal-pop-foot">
        <button type="button" class="agro-ns-cal-pop-clear" data-cal-clear>Limpar</button>
        <button type="button" class="agro-ns-cal-pop-today" data-cal-today>Hoje</button>
      </div>`;
    document.body.appendChild(calPop);
    calPop.querySelector('[data-cal-nav="-1"]').addEventListener('click', (ev) => {
      ev.stopPropagation();
      if (!calView) return;
      calView.setMonth(calView.getMonth() - 1);
      calRender();
    });
    calPop.querySelector('[data-cal-nav="1"]').addEventListener('click', (ev) => {
      ev.stopPropagation();
      if (!calView) return;
      calView.setMonth(calView.getMonth() + 1);
      calRender();
    });
    calPop.querySelector('[data-cal-clear]').addEventListener('click', (ev) => {
      ev.stopPropagation();
      if (calAnchor) {
        calAnchor.value = '';
        calAnchor.dispatchEvent(new Event('change', { bubbles: true }));
      }
      calClose();
    });
    calPop.querySelector('[data-cal-today]').addEventListener('click', (ev) => {
      ev.stopPropagation();
      calPick(todayISO());
    });
    calPop.addEventListener('click', (ev) => ev.stopPropagation());
    document.addEventListener('click', (ev) => {
      if (!calPop || calPop.classList.contains('hidden')) return;
      if (calPop.contains(ev.target) || ev.target === calAnchor) return;
      calClose();
    });
    document.addEventListener('keydown', (ev) => {
      if (ev.key === 'Escape' && calPop && !calPop.classList.contains('hidden')) calClose();
    });
    return calPop;
  }

  function calClose() {
    if (!calPop) return;
    calPop.classList.add('hidden');
    calAnchor = null;
  }

  function calPick(iso) {
    if (calAnchor) {
      calAnchor.value = iso;
      calAnchor.dispatchEvent(new Event('change', { bubbles: true }));
    }
    calClose();
  }

  function calRender() {
    const pop = calEnsurePop();
    if (!calView) return;
    pop.style.setProperty('--ns-accent', calAccent);
    pop.style.setProperty('--ns-accent-soft', calAccentSoft);
    const y = calView.getFullYear();
    const m = calView.getMonth();
    pop.querySelector('[data-cal-title]').textContent = `${CAL_MESES[m]} ${y}`;
    const grid = pop.querySelector('[data-cal-grid]');
    const sel = calAnchor ? String(calAnchor.value || '').slice(0, 10) : '';
    const hoje = todayISO();
    const first = new Date(y, m, 1);
    const start = new Date(y, m, 1 - first.getDay());
    let html = '';
    for (let i = 0; i < 42; i += 1) {
      const d = new Date(start);
      d.setDate(start.getDate() + i);
      const iso = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
      const cls = [
        'agro-ns-cal-pop-day',
        d.getMonth() !== m ? 'is-out' : '',
        iso === hoje ? 'is-today' : '',
        iso === sel ? 'is-sel' : '',
      ].filter(Boolean).join(' ');
      html += `<button type="button" class="${cls}" data-cal-iso="${iso}">${d.getDate()}</button>`;
    }
    grid.innerHTML = html;
    grid.querySelectorAll('[data-cal-iso]').forEach((btn) => {
      btn.addEventListener('click', (ev) => {
        ev.stopPropagation();
        calPick(btn.getAttribute('data-cal-iso'));
      });
    });
  }

  function calOpen(inp, opts) {
    const o = opts || {};
    calAccent = o.accent || '#059669';
    calAccentSoft = o.accentSoft || '#ecfdf5';
    const pop = calEnsurePop();
    calAnchor = inp;
    const v = String(inp.value || '').slice(0, 10);
    if (/^\d{4}-\d{2}-\d{2}$/.test(v)) {
      calView = new Date(v + 'T12:00:00');
    } else {
      calView = new Date();
      calView.setHours(12, 0, 0, 0);
    }
    calRender();
    pop.classList.remove('hidden');
    const r = inp.getBoundingClientRect();
    const ph = pop.offsetHeight || 360;
    const pw = pop.offsetWidth || 320;
    let top = r.bottom + 6;
    if (top + ph > window.innerHeight - 8) top = Math.max(8, r.top - ph - 6);
    let left = r.left;
    if (left + pw > window.innerWidth - 8) left = Math.max(8, window.innerWidth - pw - 8);
    pop.style.top = `${top}px`;
    pop.style.left = `${left}px`;
  }

  function bindInput(inp, opts) {
    if (!inp || inp.dataset.agroDateBound === '1') return;
    inp.dataset.agroDateBound = '1';
    inp.classList.add('agro-ns-input-date');
    inp.addEventListener('click', (ev) => {
      ev.preventDefault();
      ev.stopPropagation();
      calOpen(inp, opts);
    });
    inp.addEventListener('focus', (ev) => {
      ev.preventDefault();
      inp.blur();
      calOpen(inp, opts);
    });
  }

  function bind(root, opts) {
    (root || document).querySelectorAll('input[type="date"].agro-date-picker, input[type="date"][data-agro-date-picker]').forEach((inp) => {
      bindInput(inp, opts);
    });
  }

  window.AgroDatePicker = { bind, bindInput, calOpen };
})();
