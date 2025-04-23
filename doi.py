import re
import time
import requests
import datetime
import bibtexparser
from bibtexparser.bparser import BibTexParser
from bibtexparser.customization import homogenize_latex_encoding
from bibtexparser.bwriter import BibTexWriter
from bibtexparser.bibdatabase import BibDatabase

# ─── Constants ────────────────────────────────────────────────────────────────
DOI2BIB_URL        = "https://doi2bib.org/bib/"
CROSSREF_API_WORKS = "https://api.crossref.org/works"
ARXIV_ABS_URL      = "https://arxiv.org/abs/"
MAX_RETRIES        = 3
BACKOFF_FACTOR     = 1  # seconds

# Track which DOIs we've already injected, to prevent duplicates
used_dois = set()

# ─── HTTP helper with rate‑limit backoff ────────────────────────────────────────
def http_get(url, **kwargs):
    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.get(url, **kwargs)
        if resp.status_code == 429:
            wait = BACKOFF_FACTOR * (2 ** (attempt - 1))
            print(f"WARNING: 429 from {url}, retrying in {wait}s…")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp
    resp.raise_for_status()

# ─── BibTeX text cleanup ──────────────────────────────────────────────────────
def clean_protected_case(bibtex_str: str) -> str:
    # Merge double‑braced initials into single braces, then strip
    bibtex_str = re.sub(
        r"(title\s*=\s*)\{\{([A-Za-z])\}(.*?)\}",
        lambda m: f"{m.group(1)}{{{m.group(2)+m.group(3)}}}",
        bibtex_str,
        flags=re.DOTALL
    )
    bibtex_str = re.sub(r"\{([A-Za-z])\}", r"\1", bibtex_str)
    return bibtex_str

# ─── Fetch BibTeX by DOI via doi2bib.org (with CrossRef fallback) ─────────────
def fetch_bibtex_from_doi(doi: str) -> str:
    url = DOI2BIB_URL + doi
    try:
        text = http_get(url).text
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            print(f" doi2bib.org miss for {doi}, using CrossRef fallback")
            url2 = f"{CROSSREF_API_WORKS}/{doi}/transform/application/x-bibtex"
            text = http_get(url2, headers={'Accept': 'application/x-bibtex'}).text
        else:
            raise
    return clean_protected_case(text)

# ─── Search CrossRef by title+author, require exact title match ───────────────
def search_doi_by_metadata(title: str, author: str) -> str:
    title_clean = re.sub(r"[\\\{\}]", "", title).strip()
    params = {
        'query.title':  title_clean,
        'query.author': author or "",
        'rows':         5
    }
    resp = http_get(CROSSREF_API_WORKS, params=params,
                    headers={'Accept': 'application/json'})
    items = resp.json().get('message', {}).get('items', [])
    if not items:
        raise ValueError(f"No DOI candidates for “{title_clean}”")

    def normalize(s: str) -> str:
        return re.sub(r"\W+", "", s).lower()

    target_norm = normalize(title_clean)
    for item in items:
        cand = item.get('title', [""])[0]
        if normalize(cand) == target_norm:
            return item['DOI']

    raise ValueError(f"No exact‑match DOI for “{title_clean}”")

# ─── If an entry links to arXiv, grab the DOI from the abstract page ────────────
def extract_arxiv_doi(arxiv_id: str) -> str:
    resp = http_get(ARXIV_ABS_URL + arxiv_id)
    match = re.search(r'href="(https?://doi.org/[^"]+)"', resp.text)
    if not match:
        raise ValueError(f"No journal DOI on arXiv page {arxiv_id}")
    return match.group(1).split('/')[-1]

# ─── Main enrichment pipeline ─────────────────────────────────────────────────
def process_bib_file(input_path: str, output_path: str):
    raw_text = open(input_path, encoding='utf-8', errors='ignore').read()

    entry_pattern = re.compile(
        r'@(?P<type>\w+)\{(?P<key>[^,]+),(?P<body>.*?)(?=(^@\w+\{)|\Z)',
        re.DOTALL | re.MULTILINE
    )

    # Load structured entries for easy field access
    parser     = BibTexParser()
    parser.customization = homogenize_latex_encoding
    structured = {
        e['ID']: e
        for e in bibtexparser.load(
            open(input_path, encoding='utf-8', errors='ignore'),
            parser
        ).entries
    }
    writer = BibTexWriter()
    replacements = []

    for m in entry_pattern.finditer(raw_text):
        key        = m.group('key')
        start, end = m.span()
        ent        = structured.get(key)
        repl       = None

        if ent:
            doi  = ent.get('doi')
            urlf = ent.get('url', '').strip()

            # ─── 1) URL‑only → format as @misc ──────────────────────────────────
            if not doi and urlf and "doi.org" not in urlf and "arxiv.org/abs" not in urlf:
                author = ent.get('author', '')
                title  = ent.get('title', '').replace('\n',' ').strip()
                year   = ent.get('year', datetime.date.today().year)
                today  = datetime.date.today().isoformat()
                misc = (
                    f"@misc{{{key},\n"
                    f"  author       = {{{author}}},\n"
                    f"  title        = {{{title}}},\n"
                    f"  howpublished = {{\\url{{{urlf}}}}},\n"
                    f"  year         = {{{year}}},\n"
                    f"  note         = {{Accessed: {today}}},\n"
                    f"}}\n"
                )
                repl = clean_protected_case(misc)

            # ─── 2) ArXiv URL → resolve to DOI → fetch BibTeX ────────────────
            if repl is None and not doi and 'arxiv.org/abs' in urlf:
                aid = urlf.rstrip('/').split('/')[-1]
                try:
                    arxiv_doi = extract_arxiv_doi(aid)
                    if arxiv_doi not in used_dois:
                        repl = fetch_bibtex_from_doi(arxiv_doi)
                        used_dois.add(arxiv_doi)
                except Exception:
                    pass

            # ─── 3) No DOI → metadata search with exact title match ───────────
            if repl is None and not doi:
                try:
                    found = search_doi_by_metadata(ent.get('title',''),
                                                   ent.get('author',''))
                    if found not in used_dois:
                        repl = fetch_bibtex_from_doi(found)
                        used_dois.add(found)
                    else:
                        print(f" SKIPPING duplicate DOI {found} for {key}")
                except Exception as e:
                    print(f" • metadata search failed for {key}: {e}")

            # ─── 4) Direct DOI present → fetch BibTeX ─────────────────────────
            if repl is None and doi:
                if doi not in used_dois:
                    repl = fetch_bibtex_from_doi(doi)
                    used_dois.add(doi)

            # ─── 5) Fallback to original formatting ───────────────────────────
            if repl is None:
                db = BibDatabase()
                db.entries = [ent]
                repl = clean_protected_case(writer.write(db))

        else:
            # ─── Raw block: try to extract DOI or URL and fetch, else clean ───
            block = m.group(0)
            m1 = re.search(r'DOI\s*=\s*\{([^}]+)\}', block, re.IGNORECASE)
            doi = m1.group(1) if m1 else None
            if not doi:
                m2 = re.search(r'url\s*=\s*\{([^}]+)\}', block, re.IGNORECASE)
                if m2 and 'doi.org' in m2.group(1):
                    doi = m2.group(1).split('doi.org/')[-1]
            if doi and doi not in used_dois:
                try:
                    repl = fetch_bibtex_from_doi(doi)
                    used_dois.add(doi)
                except Exception:
                    pass
            if repl is None:
                repl = clean_protected_case(block)

        replacements.append({'start': start, 'end': end, 'text': repl})

    # Apply replacements from bottom to top
    new_text = raw_text
    for r in sorted(replacements, key=lambda x: x['start'], reverse=True):
        new_text = new_text[:r['start']] + r['text'] + new_text[r['end']:]

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(new_text)
    print(f"Finished writing enriched .bib → {output_path}")

# ─── CLI entry point ──────────────────────────────────────────────────────────
if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser(
        description="Enrich a .bib: handle URL‑only (@misc), ArXiv, DOI lookups, exact-title checks, and duplicate‑DOI guards."
    )
    p.add_argument('input_bib',  help='Path to your input .bib file')
    p.add_argument('output_bib', help='Path to write the updated .bib file')
    args = p.parse_args()
    process_bib_file(args.input_bib, args.output_bib)
