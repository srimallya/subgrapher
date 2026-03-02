const { ipcRenderer } = require('electron');

let markerModeEnabled = false;
let syncedHighlights = [];

function canonicalizeUrl(rawUrl) {
  const target = String(rawUrl || '').trim();
  if (!target) return '';
  try {
    const parsed = new URL(target);
    let path = parsed.pathname || '/';
    if (path.length > 1 && path.endsWith('/')) path = path.slice(0, -1);
    return `${parsed.protocol}//${parsed.host}${path}${parsed.search}`;
  } catch (_) {
    return target.toLowerCase();
  }
}

function ensureMarkerStyles() {
  if (!document || document.getElementById('subgrapher-marker-style')) return;
  const style = document.createElement('style');
  style.id = 'subgrapher-marker-style';
  style.textContent = `
    mark[data-subgrapher-marker-id] {
      background: rgba(255, 230, 128, 0.62);
      color: inherit;
      border-radius: 2px;
      box-shadow: inset 0 0 0 1px rgba(194, 150, 30, 0.32);
      padding: 0;
    }
  `;
  (document.head || document.documentElement).appendChild(style);
}

function isIgnoredNode(node) {
  if (!node) return true;
  if (node.nodeType !== Node.ELEMENT_NODE) return false;
  const tagName = String(node.tagName || '').toLowerCase();
  return tagName === 'script' || tagName === 'style' || tagName === 'noscript';
}

function unwrapMarkerElement(mark) {
  if (!mark || !mark.parentNode) return;
  const parent = mark.parentNode;
  while (mark.firstChild) parent.insertBefore(mark.firstChild, mark);
  parent.removeChild(mark);
}

function clearAppliedHighlights() {
  const marks = document.querySelectorAll('mark[data-subgrapher-marker-id]');
  marks.forEach((mark) => unwrapMarkerElement(mark));
}

function getNearestEditableElement(node) {
  let current = node;
  while (current) {
    if (current.nodeType === Node.ELEMENT_NODE) {
      const el = current;
      const tag = String(el.tagName || '').toLowerCase();
      if (tag === 'input' || tag === 'textarea') return el;
      if (el.isContentEditable) return el;
    }
    current = current.parentNode;
  }
  return null;
}

function getDocumentRoot() {
  if (!document) return null;
  return document.body || document.documentElement || null;
}

function collectTextSegments(options = {}) {
  const includeMarkedText = !!options.includeMarkedText;
  const root = getDocumentRoot();
  if (!root) return { text: '', segments: [] };
  const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT, {
    acceptNode(node) {
      if (!node || !node.nodeValue) return NodeFilter.FILTER_REJECT;
      let parent = node.parentNode;
      while (parent) {
        if (parent.nodeType === Node.ELEMENT_NODE) {
          const el = parent;
          if (isIgnoredNode(el)) return NodeFilter.FILTER_REJECT;
          if (!includeMarkedText && el.matches && el.matches('mark[data-subgrapher-marker-id]')) return NodeFilter.FILTER_REJECT;
        }
        parent = parent.parentNode;
      }
      return NodeFilter.FILTER_ACCEPT;
    },
  });

  const segments = [];
  let text = '';
  let current = walker.nextNode();
  while (current) {
    const value = String(current.nodeValue || '');
    if (value) {
      const start = text.length;
      text += value;
      segments.push({ node: current, start, end: text.length });
    }
    current = walker.nextNode();
  }
  return { text, segments };
}

function resolveTextIndexPosition(segments, index) {
  if (!Array.isArray(segments) || segments.length === 0) return null;
  for (const segment of segments) {
    if (index < segment.end) {
      return {
        node: segment.node,
        offset: Math.max(0, index - segment.start),
      };
    }
  }
  const last = segments[segments.length - 1];
  return {
    node: last.node,
    offset: Math.max(0, String(last.node.nodeValue || '').length),
  };
}

function getAbsoluteTextIndex(node, offset) {
  const root = getDocumentRoot();
  if (!root) return null;
  try {
    const range = document.createRange();
    range.selectNodeContents(root);
    range.setEnd(node, offset);
    return Math.max(0, String(range.toString() || '').length);
  } catch (_) {
    return null;
  }
}

function getRangeAbsoluteBounds(range) {
  if (!range || range.collapsed) return null;
  const startRaw = getAbsoluteTextIndex(range.startContainer, range.startOffset);
  const endRaw = getAbsoluteTextIndex(range.endContainer, range.endOffset);
  if (!Number.isFinite(startRaw) || !Number.isFinite(endRaw)) return null;
  const start = Math.max(0, Math.min(startRaw, endRaw));
  const end = Math.max(start, Math.max(startRaw, endRaw));
  if (end <= start) return null;
  return { start, end };
}

function reconcileBoundsWithText(bounds, targetText, corpusText) {
  const corpus = String(corpusText || '');
  const needle = String(targetText || '');
  if (!needle || !corpus) return null;

  const normalizeBounds = (candidate) => {
    if (!candidate) return null;
    const start = Math.max(0, Math.min(Math.round(Number(candidate.start) || 0), corpus.length));
    const end = Math.max(start, Math.min(Math.round(Number(candidate.end) || 0), corpus.length));
    if (end <= start) return null;
    return { start, end };
  };

  const bounded = normalizeBounds(bounds);
  if (bounded && corpus.slice(bounded.start, bounded.end) === needle) {
    return bounded;
  }

  const hits = [];
  let from = 0;
  while (from <= corpus.length) {
    const pos = corpus.indexOf(needle, from);
    if (pos < 0) break;
    hits.push(pos);
    from = pos + 1;
    if (hits.length > 800) break;
  }
  if (!hits.length) return null;

  const preferredStart = bounded ? bounded.start : 0;
  let picked = hits[0];
  let bestDistance = Math.abs(hits[0] - preferredStart);
  for (let i = 1; i < hits.length; i += 1) {
    const distance = Math.abs(hits[i] - preferredStart);
    if (distance < bestDistance) {
      picked = hits[i];
      bestDistance = distance;
    }
  }
  return { start: picked, end: picked + needle.length };
}

function buildWebHighlightPayloadFromBounds(bounds, fullText, pageUrl, pageUrlNorm) {
  if (!bounds || !Number.isFinite(bounds.start) || !Number.isFinite(bounds.end)) return null;
  const textCorpus = String(fullText || '');
  const start = Math.max(0, Math.min(Math.round(bounds.start), textCorpus.length));
  const end = Math.max(start, Math.min(Math.round(bounds.end), textCorpus.length));
  if (end <= start) return null;
  const text = textCorpus.slice(start, end);
  if (!text.trim()) return null;
  return {
    source: 'web',
    url: pageUrl,
    url_norm: pageUrlNorm,
    text,
    context_before: textCorpus.slice(Math.max(0, start - 80), start),
    context_after: textCorpus.slice(end, Math.min(textCorpus.length, end + 80)),
    web_start: start,
    web_end: end,
  };
}

function getIntersectingMarkerElements(range) {
  if (!range) return [];
  const markers = Array.from(document.querySelectorAll('mark[data-subgrapher-marker-id]'));
  return markers.filter((mark) => {
    try {
      if (typeof range.intersectsNode === 'function') return range.intersectsNode(mark);
    } catch (_) { }
    try {
      const markRange = document.createRange();
      markRange.selectNodeContents(mark);
      return range.compareBoundaryPoints(Range.END_TO_START, markRange) < 0
        && range.compareBoundaryPoints(Range.START_TO_END, markRange) > 0;
    } catch (_) {
      return false;
    }
  });
}

function buildPartialUnmarkPayload(range) {
  if (!range || range.collapsed) return null;
  const root = getDocumentRoot();
  if (!root) return null;
  if (getNearestEditableElement(range.commonAncestorContainer)) return null;
  const pageUrl = String(window.location && window.location.href ? window.location.href : '');
  const pageUrlNorm = canonicalizeUrl(pageUrl);
  if (!pageUrlNorm) return null;
  const indexData = collectTextSegments({ includeMarkedText: true });
  const fullText = String((indexData && indexData.text) || '');
  const selectedText = String(range.toString() || '');
  if (!selectedText.trim()) return null;

  const bounds = reconcileBoundsWithText(getRangeAbsoluteBounds(range), selectedText, fullText);
  if (!bounds) return null;

  const affectedMarkers = getIntersectingMarkerElements(range);
  if (!affectedMarkers.length) return null;

  const removeIds = new Set();
  const additions = [];

  affectedMarkers.forEach((mark) => {
    const markerId = String(mark.getAttribute('data-subgrapher-marker-id') || '').trim();
    if (!markerId) return;
    removeIds.add(markerId);
    let markRange = null;
    try {
      markRange = document.createRange();
      markRange.selectNodeContents(mark);
    } catch (_) {
      return;
    }
    const markBounds = getRangeAbsoluteBounds(markRange);
    if (!markBounds) return;
    const overlapStart = Math.max(markBounds.start, bounds.start);
    const overlapEnd = Math.min(markBounds.end, bounds.end);
    if (overlapEnd <= overlapStart) return;
    const leftPayload = buildWebHighlightPayloadFromBounds({ start: markBounds.start, end: overlapStart }, fullText, pageUrl, pageUrlNorm);
    if (leftPayload) additions.push(leftPayload);
    const rightPayload = buildWebHighlightPayloadFromBounds({ start: overlapEnd, end: markBounds.end }, fullText, pageUrl, pageUrlNorm);
    if (rightPayload) additions.push(rightPayload);
  });

  if (!removeIds.size) return null;
  return {
    source: 'web',
    action: 'partial_unmark',
    url: pageUrl,
    url_norm: pageUrlNorm,
    remove_ids: Array.from(removeIds),
    additions,
  };
}

function findHighlightRange(highlight, indexData) {
  const directStart = Number(highlight && highlight.web_start);
  const directEnd = Number(highlight && highlight.web_end);
  if (
    indexData
    && typeof indexData.text === 'string'
    && Array.isArray(indexData.segments)
    && Number.isFinite(directStart)
    && Number.isFinite(directEnd)
    && directEnd > directStart
  ) {
    const startIndex = Math.max(0, Math.min(Math.round(directStart), indexData.text.length));
    const endIndex = Math.max(startIndex, Math.min(Math.round(directEnd), indexData.text.length));
    if (endIndex > startIndex && indexData.text.slice(startIndex, endIndex) === String((highlight && highlight.text) || '')) {
      const startPos = resolveTextIndexPosition(indexData.segments, startIndex);
      const endPos = resolveTextIndexPosition(indexData.segments, endIndex);
      if (startPos && endPos) {
        const range = document.createRange();
        try {
          range.setStart(startPos.node, Math.max(0, Math.min(startPos.offset, String(startPos.node.nodeValue || '').length)));
          range.setEnd(endPos.node, Math.max(0, Math.min(endPos.offset, String(endPos.node.nodeValue || '').length)));
          if (!range.collapsed) return { range, startIndex };
        } catch (_) {
          // fallback below
        }
      }
    }
  }

  const selectedText = String((highlight && highlight.text) || '');
  if (!selectedText || !indexData || !indexData.text) return null;
  const prefix = String((highlight && highlight.context_before) || '');
  const suffix = String((highlight && highlight.context_after) || '');

  const candidates = [];
  let searchFrom = 0;
  while (searchFrom <= indexData.text.length) {
    const hit = indexData.text.indexOf(selectedText, searchFrom);
    if (hit < 0) break;
    const beforeOk = !prefix || indexData.text.slice(Math.max(0, hit - prefix.length), hit) === prefix;
    const afterStart = hit + selectedText.length;
    const afterOk = !suffix || indexData.text.slice(afterStart, afterStart + suffix.length) === suffix;
    candidates.push({ hit, beforeOk, afterOk });
    searchFrom = hit + 1;
    if (candidates.length > 400) break;
  }
  if (!candidates.length) return null;

  const picked = candidates.find((c) => c.beforeOk && c.afterOk)
    || candidates.find((c) => c.beforeOk)
    || candidates.find((c) => c.afterOk)
    || candidates[0];

  const startPos = resolveTextIndexPosition(indexData.segments, picked.hit);
  const endPos = resolveTextIndexPosition(indexData.segments, picked.hit + selectedText.length);
  if (!startPos || !endPos) return null;

  const range = document.createRange();
  try {
    range.setStart(startPos.node, Math.max(0, Math.min(startPos.offset, String(startPos.node.nodeValue || '').length)));
    range.setEnd(endPos.node, Math.max(0, Math.min(endPos.offset, String(endPos.node.nodeValue || '').length)));
  } catch (_) {
    return null;
  }
  if (range.collapsed) return null;
  return { range, startIndex: picked.hit };
}

function applyHighlights(highlights) {
  clearAppliedHighlights();
  ensureMarkerStyles();
  const list = Array.isArray(highlights) ? highlights : [];
  if (!list.length) return;

  const indexData = collectTextSegments();
  if (!indexData.text || !Array.isArray(indexData.segments) || !indexData.segments.length) return;

  const prepared = [];
  list.forEach((item) => {
    const found = findHighlightRange(item, indexData);
    if (!found) return;
    prepared.push({
      id: String((item && item.id) || ''),
      range: found.range,
      startIndex: found.startIndex,
    });
  });

  prepared.sort((a, b) => b.startIndex - a.startIndex);
  prepared.forEach((entry) => {
    const mark = document.createElement('mark');
    mark.setAttribute('data-subgrapher-marker-id', entry.id || 'marker');
    try {
      const fragment = entry.range.extractContents();
      if (!fragment || !fragment.textContent) return;
      mark.appendChild(fragment);
      entry.range.insertNode(mark);
    } catch (_) {
      // ignore overlaps
    }
  });
}

function selectionToPayload(rangeInput) {
  const selection = window.getSelection();
  const range = rangeInput || (selection && selection.rangeCount > 0 ? selection.getRangeAt(0) : null);
  if (!range || range.collapsed) return null;
  const selectedText = String(range.toString() || '');
  if (!selectedText.trim()) return null;
  if (getNearestEditableElement(range.commonAncestorContainer)) return null;

  const root = getDocumentRoot();
  if (!root) return null;

  const corpus = collectTextSegments({ includeMarkedText: true });
  const corpusText = String((corpus && corpus.text) || '');
  const resolvedBounds = reconcileBoundsWithText(getRangeAbsoluteBounds(range), selectedText, corpusText);

  let contextBefore = '';
  let contextAfter = '';
  let start = null;
  let end = null;
  if (resolvedBounds) {
    start = resolvedBounds.start;
    end = resolvedBounds.end;
    contextBefore = corpusText.slice(Math.max(0, start - 80), start);
    contextAfter = corpusText.slice(end, Math.min(corpusText.length, end + 80));
  } else {
    const beforeRange = document.createRange();
    beforeRange.selectNodeContents(root);
    beforeRange.setEnd(range.startContainer, range.startOffset);
    const beforeText = String(beforeRange.toString() || '');

    const afterRange = document.createRange();
    afterRange.selectNodeContents(root);
    afterRange.setStart(range.endContainer, range.endOffset);
    const afterText = String(afterRange.toString() || '');
    contextBefore = beforeText.slice(-80);
    contextAfter = afterText.slice(0, 80);
  }

  const pageUrl = String(window.location && window.location.href ? window.location.href : '');
  return {
    source: 'web',
    url: pageUrl,
    url_norm: canonicalizeUrl(pageUrl),
    text: selectedText,
    context_before: contextBefore,
    context_after: contextAfter,
    web_start: start,
    web_end: end,
  };
}

function emitSelectionIfNeeded(modeSnapshot = markerModeEnabled) {
  if (!modeSnapshot) return;
  const selection = window.getSelection();
  if (!selection || selection.rangeCount === 0 || selection.isCollapsed) return;
  const range = selection.getRangeAt(0);
  if (!range || range.collapsed) return;
  const payload = buildPartialUnmarkPayload(range) || selectionToPayload(range);
  if (!payload) return;
  payload.marker_mode_snapshot = true;
  ipcRenderer.send('browser:marker:web-selection', payload);
}

function scheduleSelectionEmit() {
  const modeSnapshot = !!markerModeEnabled;
  setTimeout(() => {
    emitSelectionIfNeeded(modeSnapshot);
  }, 0);
}

function isEditableShortcutTarget(target) {
  if (!target || typeof target !== 'object') return false;
  return !!getNearestEditableElement(target);
}

function getShortcutCommandFromKeyEvent(event) {
  if (!event) return '';
  const key = String(event.key || '');
  const code = String(event.code || '');
  const lowerKey = key.toLowerCase();

  const hasCtrlOrMeta = !!(event.ctrlKey || event.metaKey);
  if (hasCtrlOrMeta && !event.altKey) {
    if (key === '0' || code === 'Digit0' || code === 'Numpad0') return 'web_zoom_reset';
    if (key === '+' || key === '=' || code === 'NumpadAdd') return 'web_zoom_in';
    if (key === '-' || key === '_' || code === 'NumpadSubtract') return 'web_zoom_out';
    return '';
  }

  if (event.shiftKey && !event.ctrlKey && !event.metaKey && !event.altKey) {
    if (key === '0' || key === ')' || code === 'Digit0' || code === 'Numpad0') return 'ui_zoom_reset';
    if (key === '+' || key === '=' || code === 'NumpadAdd') return 'ui_zoom_in';
    if (key === '-' || key === '_' || code === 'NumpadSubtract') return 'ui_zoom_out';
    if (lowerKey === 'z' || code === 'KeyZ') return 'toggle_zen';
  }
  return '';
}

ipcRenderer.on('browser:marker-mode', (_event, data) => {
  markerModeEnabled = !!(data && data.enabled);
});

ipcRenderer.on('browser:marker-sync', (_event, data) => {
  markerModeEnabled = !!(data && data.enabled);
  syncedHighlights = (data && Array.isArray(data.highlights)) ? data.highlights : [];
  applyHighlights(syncedHighlights);
});

window.addEventListener('mouseup', scheduleSelectionEmit, true);
window.addEventListener('keydown', (event) => {
  if (!event || event.defaultPrevented) return;
  if (isEditableShortcutTarget(event.target)) return;
  const command = getShortcutCommandFromKeyEvent(event);
  if (!command) return;
  event.preventDefault();
  event.stopPropagation();
  ipcRenderer.send('browser:shortcut-command', { command });
}, true);
window.addEventListener('keyup', (event) => {
  const key = String((event && event.key) || '').toLowerCase();
  if (key === 'shift' || key === 'control' || key === 'meta' || key === 'alt') return;
  scheduleSelectionEmit();
}, true);

window.addEventListener('DOMContentLoaded', () => {
  ensureMarkerStyles();
  if (Array.isArray(syncedHighlights) && syncedHighlights.length) {
    applyHighlights(syncedHighlights);
  }
});
