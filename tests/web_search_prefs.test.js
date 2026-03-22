const test = require('node:test');
const assert = require('node:assert/strict');

const {
  normalizeSearchEngine,
  normalizeResearchProvider,
  resolveInternalWebProviderHint,
} = require('../runtime/web_search_prefs');

test('normalizeSearchEngine keeps only supported engines', () => {
  assert.equal(normalizeSearchEngine('google'), 'google');
  assert.equal(normalizeSearchEngine('bing'), 'bing');
  assert.equal(normalizeSearchEngine('ddg'), 'ddg');
  assert.equal(normalizeSearchEngine('unknown'), 'ddg');
});

test('normalizeResearchProvider keeps serpapi explicit and defaults to ddg', () => {
  assert.equal(normalizeResearchProvider('serpapi'), 'serpapi');
  assert.equal(normalizeResearchProvider('ddg'), 'ddg');
  assert.equal(normalizeResearchProvider('google'), 'ddg');
});

test('internal search defaults to ddg when no explicit hint is provided', () => {
  const settings = {
    default_search_engine: 'google',
    orchestrator_web_provider: 'ddg',
  };
  assert.equal(resolveInternalWebProviderHint(settings), 'ddg');
});

test('internal search preserves serpapi when explicitly configured', () => {
  const settings = {
    default_search_engine: 'bing',
    orchestrator_web_provider: 'serpapi',
  };
  assert.equal(resolveInternalWebProviderHint(settings), 'serpapi');
});

test('explicit provider hints override stored defaults', () => {
  const settings = {
    default_search_engine: 'google',
    orchestrator_web_provider: 'ddg',
  };
  assert.equal(resolveInternalWebProviderHint(settings, 'bing'), 'bing');
  assert.equal(resolveInternalWebProviderHint(settings, 'serpapi'), 'serpapi');
  assert.equal(resolveInternalWebProviderHint(settings, 'ddg'), 'ddg');
});
