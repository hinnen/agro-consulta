'use strict';
/**
 * Regressão: etapa 5 NÃO pode tratar confirmações 1–4 como «nota finalizada com PIN».
 * Só PIN (aprovacao_wizard_em), financeiro_lancado ou bucket concluída/encerrada.
 */
const fs = require('fs');
const vm = require('vm');
const html = fs.readFileSync('produtos/templates/produtos/entrada_nota.html', 'utf8');

function expect(ok, msg) {
  if (!ok) throw new Error(msg);
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
      if (depth === 0) {
        return html.slice(start, i + 1);
      }
    }
  }
  throw new Error(`fim de ${name} não encontrado`);
}

const src =
  extractFn('entradaNfeEstoqueJaRegistrado') +
  ';\n' +
  extractFn('entradaNfeEstoqueBloqueadoNotaFinalizada') +
  ';\n';

const ctx = {
  entradaNfeRascunhoStatusUltimo: '',
  entradaNfeRascunhoBucketUltimo: '',
  entradaNfeExtraUltimo: {},
};
vm.createContext(ctx);
vm.runInContext(src, ctx);

const bloqueado = (extra, bucket, status) => {
  ctx.entradaNfeExtraUltimo = extra || {};
  ctx.entradaNfeRascunhoBucketUltimo = bucket || '';
  ctx.entradaNfeRascunhoStatusUltimo = status || '';
  return ctx.entradaNfeEstoqueBloqueadoNotaFinalizada(null);
};

expect(
  !bloqueado({
    wizard_etapa1_confirmada_em: '2026-08-29T14:00:00',
    wizard_etapa2_confirmada_em: '2026-08-29T14:10:00',
    wizard_etapa3_confirmada_em: '2026-08-29T14:20:00',
    wizard_etapa4_lote_confirmada_em: '2026-08-29T14:30:00',
  }),
  'etapas 1–4 confirmadas não podem bloquear estoque',
);

expect(
  !bloqueado({
    wizard_etapa1_confirmada_em: 'x',
    wizard_etapa2_confirmada_em: 'x',
    wizard_etapa3_confirmada_em: 'x',
    wizard_lote_pular_em: 'x',
  }),
  'pular lote + etapas 1–3 não podem bloquear estoque',
);

expect(
  bloqueado({ aprovacao_wizard_em: '2026-08-27T12:00:00' }),
  'PIN final deve bloquear',
);
expect(bloqueado({ financeiro_lancado: true }), 'financeiro_lancado deve bloquear');
expect(bloqueado({}, 'concluida'), 'bucket concluida deve bloquear');

ctx.entradaNfeRascunhoStatusUltimo = 'estoque_aplicado';
ctx.entradaNfeExtraUltimo = { aprovacao_wizard_em: 'ok' };
expect(
  !ctx.entradaNfeEstoqueBloqueadoNotaFinalizada(null),
  'com estoque aplicado não mostra caixa amarela',
);

ctx.entradaNfeRascunhoStatusUltimo = '';
ctx.entradaNfeExtraUltimo = {
  aprovacao_wizard_em: 'ok',
  estoque_pendente_liberado_em: '2026-08-27T13:00:00',
};
expect(
  !ctx.entradaNfeEstoqueBloqueadoNotaFinalizada(null),
  'após liberar pendente não bloqueia',
);

console.log('OK NF estoque bloqueio falso (7 asserts)');
