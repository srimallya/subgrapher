const LUMINO_HTML_ARTIFACT_STYLE_GUIDE = [
  'HTML artifact visual standard:',
  '- Build normal product UI, not decorative AI-dashboard UI.',
  '- Use solid surfaces, restrained contrast, muted dark or warm light palettes, and avoid blue-heavy styling unless the user asks for it.',
  '- Typography must be plain and readable: simple sans-serif, clear h1/h2 hierarchy, 14-16px body text, no eyebrow labels, no uppercase letter-spaced microcopy, no decorative slogans.',
  '- Layouts must be predictable: one clear header and main content area, optional sidebar only when the information architecture truly needs it, no detached floating shells, no gratuitous right rail.',
  '- Use a consistent spacing scale such as 4/8/12/16/24/32px, container widths around 1200-1400px max, and standard section padding around 20-30px.',
  '- Cards, panels, inputs, buttons, badges, and modals should use subtle 1px borders, 8-12px radius max, and very light shadows only when necessary.',
  '- Ban gradients, glassmorphism, glow effects, conic/radial decoration, oversized shadows, pill overload, and repeated 20-32px rounded rectangles.',
  '- Ban hero blocks inside dashboards, floating sidebars, fake premium dark mode, decorative status dots, ornamental labels, and filler copy that explains how clean the UI is.',
  '- Prefer tables, lists, timelines, split panes, and straightforward charts that match real data. Do not default to KPI-card grids, donut charts, fake charts, quota bars, or trend badges.',
  '- Keep interactions quiet: color/opacity transitions only, 100-200ms ease, no bounce, no translate hover motion, no sliding pill tabs.',
  '- Keep text left-aligned by default, preserve strong contrast, and avoid washed-out gray-blue text.',
  '- Make the document a full responsive HTML page with accessible labels, semantic structure, and CSS variables for tokens.',
].join('\n');

const LUMINO_HTML_ARTIFACT_TOOLING_SUMMARY = [
  'Follow the Lumino HTML artifact visual standard:',
  'normal product UI, restrained colors, 8-12px radii, subtle borders, simple hierarchy, no gradients/glass/glows/hero dashboards/pill badges/KPI-card-first layouts unless the user explicitly asks for them.',
].join(' ');

function escapeHtml(value = '') {
  return String(value || '').replace(/[&<>"']/g, (char) => {
    if (char === '&') return '&amp;';
    if (char === '<') return '&lt;';
    if (char === '>') return '&gt;';
    if (char === '"') return '&quot;';
    return '&#39;';
  });
}

function buildLuminoHtmlArtifactScaffold(title = 'Interactive Visualization') {
  const safeTitle = escapeHtml(title);
  return [
    '<!doctype html>',
    '<html lang="en">',
    '<head>',
    '  <meta charset="utf-8" />',
    '  <meta name="viewport" content="width=device-width, initial-scale=1" />',
    `  <title>${safeTitle}</title>`,
    '  <style>',
    '    :root {',
    '      color-scheme: dark;',
    '      --bg: #171717;',
    '      --surface: #232323;',
    '      --surface-alt: #1d1d1d;',
    '      --border: #343434;',
    '      --text: #f2eee6;',
    '      --text-muted: #b7aea0;',
    '      --accent: #a88f5f;',
    '      --accent-soft: #3a3226;',
    '      --shadow: 0 2px 8px rgba(0, 0, 0, 0.16);',
    '      --font-family: "IBM Plex Sans", "SF Pro Text", "Helvetica Neue", sans-serif;',
    '      --radius-sm: 8px;',
    '      --radius-md: 10px;',
    '      --space-1: 4px;',
    '      --space-2: 8px;',
    '      --space-3: 12px;',
    '      --space-4: 16px;',
    '      --space-5: 24px;',
    '      --space-6: 32px;',
    '      --max-width: 1180px;',
    '    }',
    '    * { box-sizing: border-box; }',
    '    html, body { margin: 0; min-height: 100%; background: var(--bg); color: var(--text); }',
    '    body { font: 15px/1.5 var(--font-family); }',
    '    a { color: inherit; }',
    '    .page { min-height: 100vh; padding: var(--space-5); }',
    '    .shell { max-width: var(--max-width); margin: 0 auto; }',
    '    .header { padding: 0 0 var(--space-5); border-bottom: 1px solid var(--border); }',
    '    .header h1 { margin: 0; font-size: 28px; line-height: 1.15; font-weight: 600; }',
    '    .header p { margin: var(--space-2) 0 0; color: var(--text-muted); }',
    '    .panel { margin-top: var(--space-5); padding: var(--space-5); background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius-md); box-shadow: var(--shadow); }',
    '    .panel h2 { margin: 0 0 var(--space-3); font-size: 18px; line-height: 1.2; }',
    '    .panel p { margin: 0 0 var(--space-3); color: var(--text-muted); }',
    '    .panel ul { margin: 0; padding-left: 20px; }',
    '    .panel li + li { margin-top: var(--space-2); }',
    '    @media (max-width: 720px) {',
    '      .page { padding: var(--space-4); }',
    '      .panel { padding: var(--space-4); }',
    '      .header h1 { font-size: 24px; }',
    '    }',
    '  </style>',
    '</head>',
    '<body>',
    '  <main class="page">',
    '    <div class="shell">',
    '      <header class="header">',
    `        <h1>${safeTitle}</h1>`,
    '        <p>Replace this starter with the requested visualization. Keep the layout plain, legible, and data-first.</p>',
    '      </header>',
    '      <section class="panel">',
    '        <h2>Starter notes</h2>',
    '        <ul>',
    '          <li>Use semantic HTML and a single clear layout.</li>',
    '          <li>Prefer tables, lists, bars, or timelines before decorative chart types.</li>',
    '          <li>Avoid gradients, floating shells, glows, hero copy, and oversized rounded corners.</li>',
    '        </ul>',
    '      </section>',
    '    </div>',
    '  </main>',
    '</body>',
    '</html>',
  ].join('\n');
}

module.exports = {
  LUMINO_HTML_ARTIFACT_STYLE_GUIDE,
  LUMINO_HTML_ARTIFACT_TOOLING_SUMMARY,
  buildLuminoHtmlArtifactScaffold,
};
