function normalizeSearchEngine(value = '') {
  const normalized = String(value || '').trim().toLowerCase();
  return ['google', 'bing', 'ddg'].includes(normalized) ? normalized : 'ddg';
}

function normalizeResearchProvider(value = '') {
  const normalized = String(value || '').trim().toLowerCase();
  return normalized === 'serpapi' ? 'serpapi' : 'ddg';
}

function resolveInternalWebProviderHint(settings = {}, explicitHint = '') {
  const hint = String(explicitHint || '').trim().toLowerCase();
  if (hint === 'serpapi') return 'serpapi';
  if (['google', 'bing', 'ddg'].includes(hint)) return hint;

  const source = (settings && typeof settings === 'object') ? settings : {};
  const researchProvider = normalizeResearchProvider(source.orchestrator_web_provider);
  if (researchProvider === 'serpapi') return 'serpapi';
  return 'ddg';
}

module.exports = {
  normalizeSearchEngine,
  normalizeResearchProvider,
  resolveInternalWebProviderHint,
};
