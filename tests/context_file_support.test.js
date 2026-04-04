const test = require('node:test');
const assert = require('node:assert/strict');

const {
  buildContextSummaryText,
  isLowSignalPreviewText,
} = require('../runtime/context_file_support');

test('buildContextSummaryText keeps readable extracted text', () => {
  const summary = buildContextSummaryText(
    'Trust Commons working notes about sovereign debt, taxation, and agrarian reform.',
    'fallback.pdf (PDF)',
    120,
  );
  assert.match(summary, /Trust Commons working notes/);
  assert.doesNotMatch(summary, /fallback\.pdf/);
});

test('buildContextSummaryText falls back when extracted text is noisy', () => {
  const noisy = "‰Ó‰}à»'nË¼Î‡1v¹1»Âçâ§i.-?¿ßt?8'EDU†Ÿ»&NÍ A§5º¥¢";
  assert.equal(isLowSignalPreviewText(noisy), true);
  const summary = buildContextSummaryText(noisy, 'libgen-li.pdf (PDF)', 120);
  assert.equal(summary, 'libgen-li.pdf (PDF)');
});

test('buildContextSummaryText falls back on long punctuation runs', () => {
  const noisy = 'abc ###### ---- ===== xyz';
  assert.equal(isLowSignalPreviewText(noisy), true);
  const summary = buildContextSummaryText(noisy, 'notes.docx (DOCX)', 120);
  assert.equal(summary, 'notes.docx (DOCX)');
});
