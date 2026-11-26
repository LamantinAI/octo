const [offset, limit] = arguments;
const moreButton = document.querySelector("#more-results");
const challenge = document.querySelector(
  "#challenge-form, .anomaly-modal, form[action*='/anomaly']",
);
const empty = document.querySelector(
  '[data-testid="no-results"], .no-results',
);
const visible = (node) => node && !node.hidden && getComputedStyle(node).display !== "none";
const state = {
  valid_origin: location.origin === "https://duckduckgo.com",
  challenge: Boolean(challenge),
  empty: Boolean(visible(empty)),
  more: Boolean(visible(moreButton) && !moreButton.disabled),
  total: 0,
  hits: [],
};

if (!state.valid_origin || state.challenge) return state;

const seen = new Set();
const clean = (node) => node?.innerText?.replace(/\s+/gu, " ").trim() ?? "";
for (const article of document.querySelectorAll('article[data-testid="result"]')) {
  const link = article.querySelector('a[data-testid="result-title-a"]');
  if (!link || !["http:", "https:"].includes(link.protocol) || seen.has(link.href)) continue;
  const title = clean(link);
  if (!title) continue;
  seen.add(link.href);
  const index = state.total++;
  if (index >= offset && index < offset + limit) {
    const snippet = article.querySelector('[data-result="snippet"], [data-testid="result-snippet"]');
    state.hits.push({ title, url: link.href, snippet: clean(snippet) });
  }
}
if (state.total === 0) {
  state.empty ||= /(?:^|\n)No results found for /i.test(document.body?.innerText ?? "");
}
return state;
