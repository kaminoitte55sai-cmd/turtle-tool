"""株探「今週の【上場来高値銘柄】」記事の取得処理（スクレイピングのみ）。

責務はここに閉じる:
  - 対象記事の探索（どの URL を取りに行くか）
  - 記事 HTML のパース（記事日付 / タイトル / 銘柄コード / 銘柄名）

DB も Streamlit も import しない。呼び出し側から「取得済み記事 ID の集合」を
受け取り、パース結果を返すだけの純粋な取得層。これにより
取得処理・DB処理・表示処理が完全に分離され、将来 J-Quants / Yahoo Finance /
TradingView 連携を足す際も、同じ形の `collect()` を持つ兄弟モジュールを
modules/ 配下に増やすだけで済む。

■ robots.txt の遵守について
https://kabutan.jp/robots.txt は以下を宣言している:
    Disallow: /search*      -> 検索エンドポイントは使わない
    Crawl-delay: 3          -> リクエスト間隔を 3 秒以上あける
本モジュールは検索を一切使わず、サイトマップと公開ニュース一覧のみを辿り、
全リクエストで CRAWL_DELAY 秒のインターバルを守る。

■ 対象記事の探し方（実測に基づく 3 経路）
株探は「検索」も「1 か月より前のニュース一覧」も使えないため、
到達手段ごとに次の 3 つを併用する。いずれも新しい順に試し、
必要件数が揃った時点で打ち切る。

  1. ニュースサイトマップ（sitemap-news*.xml / sitemap-day*.xml）
     <news:title> を含むのでタイトルで直接一致。直近数日ぶんをほぼ 1 リクエストで拾える。

  2. ニュース一覧の日付指定（?date=YYYYMMDD&page=N）
     タイトルが載るので確実。ただし実測で遡れるのは約 1 か月（2026-07 時点で
     2026-06-27 まで）。土曜だけを対象に 1〜3 ページ見れば当たる。

  3. 過去サイトマップ（sitemap-prev*.xml）
     タイトルが無い。ただし 1 記事につき `/stock/news?code=XXXX&b=<記事ID>` が
     掲載銘柄数ぶん並ぶため、「土曜日で最も多くの銘柄コードが紐づく記事」＝
     上場来高値記事 になる。実測でサイトマップ上のコード数とタイトルの
     「〜など NN 銘柄」は完全一致した（4 週ぶんで検証、命中率 4/4）。

■ 取得できない期間について（株探側の制約）
prev サイトマップの収録は 2025-05-26 〜 2026-03-16、日付指定で遡れるのは
約 1 か月ぶんで、その間（2026-03-17 〜 2026-06-26 ごろ）は株探がどの経路でも
公開していないため取得できない。将来この期間の記事が別経路で参照可能に
なった場合は、discover_candidates に 4 つ目の経路を足せばよい。
"""

from __future__ import annotations

import datetime as dt
import html
import re
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field

import requests

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

BASE = "https://kabutan.jp"
SITEMAP_INDEX = f"{BASE}/sitemap-index.xml"

# robots.txt の Crawl-delay: 3 を遵守
CRAWL_DELAY = 3.0

# 探す記事タイトル。株探が表記を変えた場合はここだけ直せばよい。
TARGET_TITLE = "今週の【上場来高値銘柄】"

# 週次記事は土曜の朝に公開される（実測: 2025-05-31 / 06-07 / 06-14 / 06-21 /
# 2026-07-25 いずれも土曜）。日曜を候補に含めるとコラム記事ばかり拾って
# 無駄な取得が増えたため、土曜のみに限定する。
# （Python の weekday(): 月=0 … 土=5, 日=6）
# なお直近の記事はニュースサイトマップのタイトル一致で曜日に関係なく拾えるので、
# 公開曜日がずれた週を取りこぼすのは過去分の探索だけに限られる。
CANDIDATE_WEEKDAYS = {5}

# 1 日あたり何件まで「コード数が多い記事」を試すか。
# 実測では 1 件目で当たるが、話題株ダイジェスト等が上に来た場合の保険。
TOP_N_PER_DAY = 2

# ヒューリスティックの足切り。上場来高値記事は毎回 50〜85 銘柄が紐づくため、
# これを下回る記事は対象ではありえない。コラム等の誤検出を防ぐ。
MIN_CODES = 25

UA = "Mozilla/5.0 (compatible; turtle-tool/1.0; personal research)"

# --- 正規表現（実 HTML で検証済み） -----------------------------------------

# <time class="s_news_date" datetime="2026-07-25T09:00:00+09:00" itemprop="datePublished">
# 記事ページにはサイドバー等の <time> も複数あるため、記事本文の日付を示す
# class="s_news_date" / itemprop="datePublished" で必ず限定すること。
# 属性の並び順に依存しないよう、まずタグ全体を掴んでから datetime を取り出す。
RE_TIME_TAG = re.compile(r"<time[^>]*(?:s_news_date|datePublished)[^>]*>")
RE_DATETIME_ATTR = re.compile(r'datetime="(\d{4}-\d{2}-\d{2})')
# <meta name="PublicationDate" content="2026/07/25 09:00:00" />
RE_PUBDATE = re.compile(r'name="PublicationDate"\s+content="(\d{4})/(\d{2})/(\d{2})')
RE_TITLE = re.compile(r"<title>(.*?)</title>", re.S)

# 記事本文の銘柄行:
#   &lt;<a href="/stock/?code=2805">2805</a>&gt; エスビー [東証Ｓ]<br />
# 日本株の新形式コード（例 160A）にも対応するため英数字を許容する。
RE_STOCK = re.compile(
    r'&lt;\s*<a[^>]+href="/stock/\?code=([0-9A-Za-z]{4,5})"[^>]*>.*?</a>\s*&gt;'
    r"\s*([^\[<]{1,40}?)\s*\[([^\]]{1,20})\]"
)

# 業種見出し:  ● 電気機器――――――――――　 3銘柄
# 区切りは U+2015(―) の連続。業種名側からは U+2015 / U+2500 のみを除外する。
# （長音符 U+30FC を除外すると「サービス業」「その他製品」等が拾えなくなるので注意）
RE_SECTOR = re.compile(
    r"●\s*([^<>\n―─]+?)[―─\s\-]*[\d０-９]+\s*銘柄"
)

# サイトマップ内の記事URL / 銘柄ひもづけURL
RE_LOC = re.compile(r"<loc>(.*?)</loc>")
RE_STOCK_NEWS = re.compile(r"/stock/news\?code=([0-9A-Za-z]+)&(?:amp;)?b=n(\d{12})")
RE_NEWS_ENTRY = re.compile(
    r"<loc>([^<]*?b=n(\d{12}))</loc>.*?<news:title>(.*?)</news:title>", re.S
)

# ニュース一覧ページの行:  <a href="/news/marketnews/?&b=n202607250151">タイトル</a>
RE_LIST_ROW = re.compile(
    r'<a href="/news/marketnews/\?[^"]*b=n(\d{8}\d{4})"[^>]*>(.*?)</a>', re.S
)


# ---------------------------------------------------------------------------
# データ構造
# ---------------------------------------------------------------------------


@dataclass
class Stock:
    code: str
    name: str
    sector: str | None = None
    market: str | None = None


@dataclass
class Article:
    """パース済みの 1 記事。"""

    article_id: str  # 'n202607250151'
    url: str
    article_date: str  # 'YYYY-MM-DD'
    title: str
    stocks: list[Stock] = field(default_factory=list)

    def to_rows(self) -> list[dict]:
        """DB 層（modules/db.insert_rows）へ渡す行形式に変換する。"""
        return [
            {
                "article_date": self.article_date,
                "code": s.code,
                "name": s.name,
                "title": self.title,
                "sector": s.sector,
                "market": s.market,
            }
            for s in self.stocks
        ]


# ---------------------------------------------------------------------------
# HTTP（リトライ + Crawl-delay）
# ---------------------------------------------------------------------------


class Fetcher:
    """Crawl-delay を守りつつリトライする薄い HTTP クライアント。"""

    def __init__(self, delay: float = CRAWL_DELAY, retries: int = 3):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": UA})
        self.delay = delay
        self.retries = retries
        self._last_request_at = 0.0
        # 失敗の理由を必ず記録する。握りつぶすと「0件でした」としか出せず、
        # 実行環境からアクセスが弾かれているのか、単に新着が無いのか区別できない。
        self.n_requests = 0
        self.n_ok = 0
        self.errors: Counter = Counter()  # '403' / 'Timeout' などの内訳
        self.last_error: str | None = None

    def _wait(self) -> None:
        """前回リクエストから delay 秒あける。"""
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)

    def get(self, url: str, timeout: int = 30) -> str | None:
        """本文を返す。全リトライが失敗したら None（呼び出し側でスキップ）。"""
        for attempt in range(self.retries):
            self._wait()
            self.n_requests += 1
            try:
                r = self.session.get(url, timeout=timeout)
                self._last_request_at = time.monotonic()
                if r.status_code == 200:
                    # 株探は UTF-8 だが requests の推定が外れることがあるため明示する
                    r.encoding = "utf-8"
                    self.n_ok += 1
                    return r.text
                self.errors[f"HTTP {r.status_code}"] += 1
                self.last_error = f"HTTP {r.status_code} : {url}"
                # 404 は「記事が存在しない」= リトライ不要
                if r.status_code == 404:
                    return None
            except requests.RequestException as e:
                self._last_request_at = time.monotonic()
                self.errors[type(e).__name__] += 1
                self.last_error = f"{type(e).__name__} : {url}"
            # 指数バックオフ（通信エラー時のリトライ）
            if attempt < self.retries - 1:
                time.sleep(self.delay * (attempt + 1))
        return None


# ---------------------------------------------------------------------------
# パース
# ---------------------------------------------------------------------------


def _strip_tags(s: str) -> str:
    return html.unescape(re.sub(r"<[^>]+>", "", s)).strip()


def parse_article(article_id: str, url: str, page_html: str) -> Article | None:
    """記事 HTML から Article を組み立てる。対象記事でなければ None。"""
    m = RE_TITLE.search(page_html)
    if not m:
        return None
    # "今週の【上場来高値銘柄】… | 特集 - 株探ニュース" -> 先頭部分だけ使う
    title = _strip_tags(m.group(1)).split(" | ")[0].strip()

    if TARGET_TITLE not in title:
        return None  # 候補が外れだった（呼び出し側で 'skipped' として記録される）

    # --- 記事日付 -----------------------------------------------------------
    article_date = None
    if (tag := RE_TIME_TAG.search(page_html)) and (
        m2 := RE_DATETIME_ATTR.search(tag.group(0))
    ):
        article_date = m2.group(1)
    elif m3 := RE_PUBDATE.search(page_html):
        article_date = f"{m3.group(1)}-{m3.group(2)}-{m3.group(3)}"
    else:
        # 最終手段: 記事 ID 先頭 8 桁が YYYYMMDD
        digits = article_id.lstrip("n")[:8]
        if len(digits) == 8 and digits.isdigit():
            article_date = f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
    if not article_date:
        return None

    # --- 銘柄一覧 -----------------------------------------------------------
    # 冒頭の本文にも銘柄リンクが現れるが、業種別の一覧が正式な全件リスト。
    # 見出し以降だけを対象にすることで、本文中の言及との重複を避ける。
    body = page_html
    marker = body.find("◆業種別")
    if marker > 0:
        body = body[marker:]

    # 業種見出しの位置を先に拾い、各銘柄の直前にある見出しを業種として割り当てる
    sectors = [(m.start(), _strip_tags(m.group(1))) for m in RE_SECTOR.finditer(body)]

    stocks: list[Stock] = []
    seen: set[str] = set()
    for sm in RE_STOCK.finditer(body):
        code = sm.group(1).strip()
        name = html.unescape(sm.group(2)).strip()
        market = html.unescape(sm.group(3)).strip()
        if not code or code in seen:
            continue
        seen.add(code)
        # 直前の業種見出しを探す
        sector = None
        for pos, sec in sectors:
            if pos < sm.start():
                sector = sec
            else:
                break
        stocks.append(Stock(code=code, name=name, sector=sector, market=market))

    if not stocks:
        return None

    return Article(
        article_id=article_id, url=url, article_date=article_date, title=title, stocks=stocks
    )


# ---------------------------------------------------------------------------
# 探索
# ---------------------------------------------------------------------------


def _article_url(article_id: str) -> str:
    return f"{BASE}/news/marketnews/?b={article_id}"


def _sitemap_urls(fetcher: Fetcher) -> list[str]:
    """サイトマップインデックスから、ニュース系サイトマップの URL を新しい順に返す。"""
    xml = fetcher.get(SITEMAP_INDEX)
    if not xml:
        return []
    urls = RE_LOC.findall(xml)
    # base*（銘柄ページ等）はニュースを含まないので除外する
    news_maps = [u for u in urls if "sitemap-base" not in u]

    # 新しい順に並べる: news1 -> day1 -> prev11 -> prev10 -> … -> prev1
    # prev は「番号が大きいほど新しい」（実測: prev1=2025-05-26〜, prev11=〜2026-03-16）。
    # ここを昇順にすると古い期間から埋めてしまい、直近の取りこぼしに繋がるので注意。
    def rank(u: str) -> tuple[int, int]:
        m = re.search(r"sitemap-([a-z]+)(\d+)\.xml", u)
        if not m:
            return (9, 0)
        kind, num = m.group(1), int(m.group(2))
        order = {"news": 0, "day": 1, "prev": 2}.get(kind, 9)
        return (order, -num if kind == "prev" else num)

    return sorted(news_maps, key=rank)


def _recent_saturdays(weeks: int, today: dt.date | None = None) -> list[str]:
    """直近の土曜日を新しい順に 'YYYYMMDD' で返す。"""
    today = today or dt.date.today()
    # 直近（当日を含む）の土曜日まで戻る
    last_sat = today - dt.timedelta(days=(today.weekday() - 5) % 7)
    return [(last_sat - dt.timedelta(weeks=w)).strftime("%Y%m%d") for w in range(weeks)]


def _discover_from_date_pages(
    fetcher: Fetcher,
    known_ids: set[str],
    seen_ids: set[str],
    weeks: int = 10,
    pages_per_day: int = 3,
) -> list[tuple[str, str]]:
    """経路2: ニュース一覧の日付指定から、タイトル一致で記事 ID を拾う。

    `?date=` は約 1 か月ぶんしか遡れないため、指定日の記事が 1 件も
    返らなくなった時点で以降の週は諦めて打ち切る（無駄打ちの防止）。
    """
    found: list[tuple[str, str]] = []

    for day in _recent_saturdays(weeks):
        day_has_rows = False

        for page in range(1, pages_per_day + 1):
            url = f"{BASE}/news/marketnews/?date={day}"
            if page > 1:
                url += f"&page={page}"
            html_text = fetcher.get(url)
            if not html_text:
                continue

            hit_this_day = False
            for aid, raw_title in RE_LIST_ROW.findall(html_text):
                # サイドバー等に混ざる別日の記事を除外し、指定日の行だけ見る
                if not aid.startswith(day):
                    continue
                day_has_rows = True
                if TARGET_TITLE not in _strip_tags(raw_title):
                    continue
                key = f"n{aid}"
                if key not in known_ids and key not in seen_ids:
                    seen_ids.add(key)
                    found.append((key, "date-list"))
                hit_this_day = True

            if hit_this_day:
                break  # その週は見つかったので次の週へ

        if not day_has_rows:
            # `?date=` の有効範囲を抜けた -> これ以上遡っても無駄
            break

    return found


def discover_candidates(
    fetcher: Fetcher,
    known_ids: set[str],
    known_dates: set[str] | None = None,
    max_sitemaps: int = 13,
    want: int | None = None,
    deep_history: bool = False,
    progress_cb=None,
) -> list[tuple[str, str]]:
    """対象記事の候補 [(article_id, 判定理由), ...] を新しい順に返す。

    known_ids に含まれる記事は最初から除外するので、2 回目以降の実行は
    「新しく増えた記事だけ」を取りに行くことになる。

    want を指定すると、その件数の候補が貯まった時点で以降のサイトマップを
    読まずに打ち切る。サイトマップは新しい順に並べてあるため、
    週次の増分実行では最初の 1 本（sitemap-news1）だけで済むことが多い。
    """
    known_dates = known_dates or set()
    candidates: list[tuple[str, str]] = []
    seen_ids: set[str] = set()

    def enough() -> bool:
        return want is not None and len(candidates) >= want

    sitemaps = _sitemap_urls(fetcher)[:max_sitemaps]
    news_maps = [u for u in sitemaps if "sitemap-news" in u or "sitemap-day" in u]
    # 経路3（過去サイトマップの走査）は 1 本あたり数 MB あり時間がかかるので、
    # 直近数週ぶんが欲しいだけなら不要。明示的に要求されたときだけ走らせる。
    prev_maps = [u for u in sitemaps if u not in news_maps] if deep_history else []
    total_steps = len(news_maps) + 1 + len(prev_maps)
    step = 0

    # === 経路1: ニュースサイトマップ（タイトル一致・最新） ===
    for sm_url in news_maps:
        step += 1
        if enough():
            break
        if progress_cb:
            progress_cb(step, total_steps, "最新ニュースを確認中…")
        xml = fetcher.get(sm_url)
        if not xml:
            continue  # 取得失敗はスキップして次へ（途中で止めない）
        for _loc, aid, title in RE_NEWS_ENTRY.findall(xml):
            if TARGET_TITLE in html.unescape(title):
                key = f"n{aid}"
                if key not in known_ids and key not in seen_ids:
                    seen_ids.add(key)
                    candidates.append((key, "title"))

    # === 経路2: 日付指定の一覧（タイトル一致・直近約1か月） ===
    step += 1
    if not enough():
        if progress_cb:
            progress_cb(step, total_steps, "直近の一覧を確認中…")
        candidates += _discover_from_date_pages(fetcher, known_ids, seen_ids)

    # === 経路3: 過去サイトマップ（銘柄ひもづけ数のヒューリスティック） ===
    for sm_url in prev_maps:
        step += 1
        if enough():
            break
        if progress_cb:
            progress_cb(step, total_steps, "過去記事を探索中…")
        xml = fetcher.get(sm_url)
        if not xml:
            continue

        by_article: dict[str, set[str]] = defaultdict(set)
        for code, aid in RE_STOCK_NEWS.findall(xml):
            by_article[aid].add(code)
        if not by_article:
            continue

        # 日付ごとに「紐づく銘柄数が多い順」で上位を候補にする
        per_day: dict[str, list[tuple[int, str]]] = defaultdict(list)
        for aid, codes in by_article.items():
            if len(codes) < MIN_CODES:
                continue  # 銘柄数が少なすぎる記事は対象ではありえない
            try:
                d = dt.date(int(aid[:4]), int(aid[4:6]), int(aid[6:8]))
            except ValueError:
                continue
            if d.weekday() not in CANDIDATE_WEEKDAYS:
                continue  # 週次記事は土曜公開
            per_day[aid[:8]].append((len(codes), aid))

        for _day, lst in sorted(per_day.items(), reverse=True):
            if _day in known_dates:
                continue  # その日の本命記事は取得済み。別候補を試す必要はない。
            lst.sort(reverse=True)  # 銘柄数の多い順
            for _n, aid in lst[:TOP_N_PER_DAY]:
                key = f"n{aid}"
                if key not in known_ids and key not in seen_ids:
                    seen_ids.add(key)
                    candidates.append((key, "code-count"))

    return candidates


# ---------------------------------------------------------------------------
# 公開 API
# ---------------------------------------------------------------------------


def collect(
    known_ids: set[str] | None = None,
    known_dates: set[str] | None = None,
    max_articles: int = 80,
    deep_history: bool = False,
    progress_cb=None,
):
    """対象記事を探索・取得して (成功記事リスト, スキップ情報, 診断情報) を返す。

    診断情報 diag には実際に投げたリクエスト数と失敗の内訳が入る。
    取得 0 件だったときに「新着が無い」のか「実行環境から株探へ到達できない」
    のかを呼び出し側で区別できるようにするためのもの。

    引数:
        known_ids    : 取得済み記事 ID。ここに含まれる記事は取りに行かない。
        max_articles : 1 回の実行で取得する記事数の上限（実行時間の上限にもなる）。
        deep_history : True にすると過去サイトマップまで走査して数か月前まで遡る。
                       False（既定）なら直近 1 か月ぶんだけを短時間で取得する。
        progress_cb  : progress_cb(current, total, message) 形式のコールバック。
                       Streamlit の進捗バー更新に使う（UI 非依存）。

    通信エラーはリトライし、それでも駄目な記事はスキップして次へ進む。
    途中で例外を投げて止まることはない。
    """
    known_ids = known_ids or set()
    known_dates = known_dates or set()
    fetcher = Fetcher()

    articles: list[Article] = []
    skipped: list[dict] = []

    def _diag(n_candidates: int) -> dict:
        return {
            "requests": fetcher.n_requests,
            "ok": fetcher.n_ok,
            "errors": dict(fetcher.errors),
            "last_error": fetcher.last_error,
            "candidates": n_candidates,
        }

    # --- 1) 候補の探索 ---
    candidates = discover_candidates(
        fetcher,
        known_ids,
        known_dates=known_dates,
        want=max_articles,
        deep_history=deep_history,
        progress_cb=progress_cb,
    )
    candidates = candidates[:max_articles]
    total = len(candidates)

    if total == 0:
        return articles, skipped, _diag(0)

    # --- 2) 候補を 1 件ずつ取得・判定 ---
    for i, (article_id, reason) in enumerate(candidates):
        if progress_cb:
            progress_cb(i, total, f"記事を取得中 {i + 1}/{total}（{article_id}）")

        url = _article_url(article_id)
        page = fetcher.get(url)
        if page is None:
            # 存在しない / 通信不能 -> スキップして続行
            skipped.append({"article_id": article_id, "url": url, "reason": "fetch_failed"})
            continue

        try:
            art = parse_article(article_id, url, page)
        except Exception as e:  # パース失敗も止めずにスキップ
            skipped.append({"article_id": article_id, "url": url, "reason": f"parse_error: {e}"})
            continue

        if art is None:
            # ヒューリスティックの外れ（別の週次記事だった等）。
            # 記録しておけば次回以降は候補から外れる。
            skipped.append({"article_id": article_id, "url": url, "reason": "not_target"})
            continue

        articles.append(art)

    if progress_cb:
        progress_cb(total, total, "完了")

    return articles, skipped, _diag(total)
