const PATH_A_DENIED = new Set([
  'global_reference_search',
  'ensure_reference_for_topic',
  'create_reference',
  'orchestrator_job_create',
  'orchestrator_job_edit',
  'orchestrator_job_pause',
  'orchestrator_job_resume',
  'orchestrator_job_delete',
  'orchestrator_job_list',
  'telegram_send',
  'telegram_status',
  'delete_reference',
  'delete_artifact',
  'remove_tab',
]);

const PATH_B_DENIED = new Set([
  'create_artifact',
  'write_markdown_artifact',
  'write_html_artifact',
  'open_web_tab',
  'add_web_highlight',
  'add_artifact_highlight',
  'clear_highlights',
  'analyze_image',
  'run_python',
  'pip_install',
  'save_skill',
  'run_skill',
  'delete_reference',
  'delete_artifact',
  'remove_tab',
]);

function normalizeToolName(name) {
  return String(name || '').trim();
}

function isPathAToolAllowed(name) {
  const n = normalizeToolName(name);
  if (!n) return false;
  return !PATH_A_DENIED.has(n);
}

function isPathBToolAllowed(name) {
  const n = normalizeToolName(name);
  if (!n) return false;
  return !PATH_B_DENIED.has(n);
}

module.exports = {
  PATH_A_DENIED,
  PATH_B_DENIED,
  isPathAToolAllowed,
  isPathBToolAllowed,
};
