'use strict';
/**
 * Path detalhado NF-ESTOQUE-BLOQUEIO-FALSO
 * — etapa 5 NÃO bloqueia por confirmação 1–4
 * — bloqueia só PIN / financeiro / bucket concluída
 * — UI do botão azul + caixa amarela alinhados
 */
const fs = require('fs');
const vm = require('vm');
const path = require('path');

const root = path.resolve(__dirname, '..');
const html = fs.readFileSync(path.join(root, 'produtos/templates/produtos/entrada_nota.html'), 'utf8');

let n = 0;
function expect(ok, msg) {
  if (!ok) throw new Error(`FAIL #${n + 1}: ${msg}`);
  n += 1;
}

function extractFn(name) {
  const start = html.indexOf(`function ${name}(`);
  expect(start >= 0, `função ${name} ausente`);
  let depth = 0;
  let i = html.indexOf('{', start);
  expect(i >= 0, `corpo ${name} ausente`);
  for (; i < html.length; i++) {
    const ch = html[i];
    if (ch === '{') depth += 1;
    else if (ch === '}') {
      depth -= 1;
      if (depth === 0) return html.slice(start, i + 1);
    }
  }
  throw new Error(`fim de ${name} não encontrado`);
}

// --- Contratos estáticos no HTML ---
expect(
  html.includes('NÃO usar confirmações das etapas 1–4'),
  'comentário de contrato ausente',
);
expect(
  !/entradaNfeEstoqueBloqueadoNotaFinalizada[\s\S]{0,800}entradaNfeWizardFluxoLegadoOuConcluido/.test(html),
  'Bloqueado ainda chama FluxoLegado (regressão)',
);
expect(html.includes("btn.textContent = 'Reabra a nota abaixo para registrar estoque'"), 'texto botão bloqueado');
expect(html.includes('nfe-wiz-estoque-reabrir-wrap'), 'wrap amarelo');
expect(html.includes("ENTRADA_NFE_BTN_ESTOQUE_PADRAO = 'REGISTRAR ENTRADA NO ESTOQUE AGRO'"), 'rótulo azul padrão');

// --- Funções de decisão ---
const src =
  extractFn('entradaNfeEstoqueJaRegistrado') +
  ';\n' +
  extractFn('entradaNfeEstoqueBloqueadoNotaFinalizada') +
  ';\n';

class FakeEl {
  constructor() {
    this.disabled = false;
    this.textContent = '';
    this.classList = {
      _s: new Set(),
      add(...xs) { xs.forEach((x) => this._s.add(x)); },
      remove(...xs) { xs.forEach((x) => this._s.delete(x)); },
      contains(x) { return this._s.has(x); },
    };
    this._attrs = {};
  }
  removeAttribute(k) { delete this._attrs[k]; }
  setAttribute(k, v) { this._attrs[k] = v; }
}

const btn = new FakeEl();
const wrap = new FakeEl();
wrap.classList.add('hidden');

const ctx = {
  entradaNfeRascunhoStatusUltimo: '',
  entradaNfeRascunhoBucketUltimo: '',
  entradaNfeExtraUltimo: {},
  ENTRADA_NFE_BTN_ESTOQUE_PADRAO: 'REGISTRAR ENTRADA NO ESTOQUE AGRO',
  document: {
    getElementById(id) {
      if (id === 'btn-estoque-agro') return btn;
      if (id === 'nfe-wiz-estoque-reabrir-wrap') return wrap;
      return null;
    },
  },
};
vm.createContext(ctx);
vm.runInContext(src, ctx);

// Extrai UI depois das funções base (precisa delas no contexto)
const uiSrc = extractFn('entradaNfeAtualizarUiBotaoEstoqueAgro');
vm.runInContext(uiSrc + ';\n', ctx);

function resetUi() {
  btn.disabled = false;
  btn.textContent = '';
  btn.classList._s.clear();
  wrap.classList._s.clear();
  wrap.classList.add('hidden');
}

function bloqueado(extra, bucket, status) {
  ctx.entradaNfeExtraUltimo = extra || {};
  ctx.entradaNfeRascunhoBucketUltimo = bucket || '';
  ctx.entradaNfeRascunhoStatusUltimo = status || '';
  return ctx.entradaNfeEstoqueBloqueadoNotaFinalizada(null);
}

function uiCom(rz) {
  resetUi();
  ctx.entradaNfeExtraUltimo = (rz && rz.extra) || {};
  ctx.entradaNfeRascunhoBucketUltimo = (rz && rz.bucket) || '';
  ctx.entradaNfeRascunhoStatusUltimo = (rz && rz.status) || '';
  ctx.entradaNfeAtualizarUiBotaoEstoqueAgro(rz || null);
  return {
    disabled: btn.disabled,
    text: btn.textContent,
    wrapHidden: wrap.classList.contains('hidden'),
  };
}

const etapas14 = {
  wizard_etapa1_confirmada_em: '2026-08-29T14:00:00',
  wizard_etapa2_confirmada_em: '2026-08-29T14:10:00',
  wizard_etapa3_confirmada_em: '2026-08-29T14:20:00',
  wizard_etapa4_lote_confirmada_em: '2026-08-29T14:30:00',
};

// Caso Renan (NF 3024907)
expect(!bloqueado(etapas14), 'etapas 1–4 não bloqueiam');
expect(
  !bloqueado({
    wizard_etapa1_confirmada_em: 'x',
    wizard_etapa2_confirmada_em: 'x',
    wizard_etapa3_confirmada_em: 'x',
    wizard_lote_pular_em: 'x',
  }),
  'pular lote + 1–3 não bloqueiam',
);

expect(bloqueado({ aprovacao_wizard_em: '2026-08-27T12:00:00' }), 'PIN bloqueia');
expect(bloqueado({ financeiro_lancado: true }), 'financeiro_lancado bloqueia');
expect(bloqueado({ financeiro_lancado: 1 }), 'financeiro truthy bloqueia');
expect(bloqueado({}, 'concluida'), 'bucket concluida bloqueia');
expect(bloqueado({}, 'encerrada'), 'bucket encerrada bloqueia');
expect(!bloqueado({}, 'rascunho'), 'bucket rascunho não bloqueia');
expect(!bloqueado({}, 'estoque'), 'bucket estoque não bloqueia');

ctx.entradaNfeRascunhoStatusUltimo = 'estoque_aplicado';
ctx.entradaNfeExtraUltimo = { aprovacao_wizard_em: 'ok' };
expect(!ctx.entradaNfeEstoqueBloqueadoNotaFinalizada(null), 'estoque_aplicado não mostra amarelo');

ctx.entradaNfeRascunhoStatusUltimo = 'encerrada';
ctx.entradaNfeExtraUltimo = { aprovacao_wizard_em: 'ok' };
expect(!ctx.entradaNfeEstoqueBloqueadoNotaFinalizada(null), 'encerrada = já registrada');

ctx.entradaNfeRascunhoStatusUltimo = '';
ctx.entradaNfeExtraUltimo = {
  aprovacao_wizard_em: 'ok',
  estoque_pendente_liberado_em: '2026-08-27T13:00:00',
};
expect(!ctx.entradaNfeEstoqueBloqueadoNotaFinalizada(null), 'liberado pendente não bloqueia');

ctx.entradaNfeExtraUltimo = { estoque_aplicado_em: '2026-08-29T15:00:00', aprovacao_wizard_em: 'ok' };
expect(!ctx.entradaNfeEstoqueBloqueadoNotaFinalizada(null), 'carimbo estoque_aplicado_em');

ctx.entradaNfeExtraUltimo = { estoque_agro_ajuste_ids: [12], aprovacao_wizard_em: 'ok' };
expect(!ctx.entradaNfeEstoqueBloqueadoNotaFinalizada(null), 'ajuste_ids conta como registrado');

ctx.entradaNfeExtraUltimo = { estoque_agro_ajuste_ids: [], aprovacao_wizard_em: 'ok' };
expect(ctx.entradaNfeEstoqueBloqueadoNotaFinalizada(null), 'ajuste_ids vazio + PIN ainda bloqueia');

// rz explícito (extra no objeto) prevalece sobre globals
ctx.entradaNfeExtraUltimo = etapas14;
expect(
  ctx.entradaNfeEstoqueBloqueadoNotaFinalizada({
    status: 'aberto',
    extra: { aprovacao_wizard_em: 'pin' },
  }),
  'rz.extra PIN sobrescreve global etapas',
);
expect(
  !ctx.entradaNfeEstoqueBloqueadoNotaFinalizada({
    status: 'aberto',
    extra: etapas14,
  }),
  'rz.extra só etapas 1–4 não bloqueia',
);

// --- UI botão ---
let u = uiCom({ status: 'aberto', extra: etapas14 });
expect(!u.disabled && u.text === 'REGISTRAR ENTRADA NO ESTOQUE AGRO' && u.wrapHidden, 'UI etapas 1–4 = botão azul livre');

u = uiCom({ status: 'aberto', extra: { aprovacao_wizard_em: 'ok' } });
expect(u.disabled && /Reabra a nota/i.test(u.text) && !u.wrapHidden, 'UI PIN = amarelo + botão travado');

u = uiCom({ status: 'aberto', extra: { financeiro_lancado: true } });
expect(u.disabled && !u.wrapHidden, 'UI financeiro = amarelo');

u = uiCom({ status: 'estoque_aplicado', extra: {} });
expect(u.disabled && /já registrado/i.test(u.text) && u.wrapHidden, 'UI estoque aplicado');

u = uiCom({ status: 'encerrada', extra: {} });
expect(u.disabled && /encerrada/i.test(u.text) && u.wrapHidden, 'UI encerrada');

u = uiCom({ status: 'descartada', extra: {} });
expect(u.disabled && /descartada/i.test(u.text) && u.wrapHidden, 'UI descartada');

u = uiCom({
  status: 'aberto',
  extra: { aprovacao_wizard_em: 'ok', estoque_pendente_liberado_em: 'x' },
});
expect(!u.disabled && u.wrapHidden && u.text === 'REGISTRAR ENTRADA NO ESTOQUE AGRO', 'UI após liberar pendente');

u = uiCom({ status: 'aberto', bucket: 'concluida', extra: {} });
expect(u.disabled && !u.wrapHidden, 'UI bucket concluida no rz');

u = uiCom({ status: 'aberto', bucket: 'encerrada', extra: {} });
expect(u.disabled && !u.wrapHidden, 'UI bucket encerrada no rz');

// Validação etapa 5: mensagem correta quando bloqueado vs quando falta registrar
expect(
  html.includes("Nota finalizada sem estoque: use «Reabrir nota» nesta etapa"),
  'msg validação bloqueado',
);
expect(
  html.includes('Registre a entrada no estoque Agro com o botão azul'),
  'msg validação falta registrar',
);

console.log(`OK NF-ESTOQUE-BLOQUEIO-FALSO path detalhado ${n}/${n}`);
