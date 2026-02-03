import type { StockPricePoint } from '../types';

export type RealtimeIndicatorsEnrichResult =
  | { ok: true; points: StockPricePoint[]; prevDaysUsed: number; today: string }
  | { ok: false; points: StockPricePoint[]; reason: string; prevDaysUsed: number; today?: string };

export function normalizeDate(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  const digits = String(value).replace(/\D/g, '');
  if (digits.length < 8) return null;
  return digits.slice(0, 8);
}

export function pickTodayDateFromIntraday(points: StockPricePoint[]): string | null {
  for (let i = points.length - 1; i >= 0; i--) {
    const d = normalizeDate(points[i]?.date);
    if (d) return d;
  }
  return null;
}

export function selectPrevDailyCloses(
  dailyPoints: StockPricePoint[],
  todayYmd: string,
  count: number,
): number[] {
  const filtered: { date: string; close: number }[] = [];
  for (const p of dailyPoints) {
    const d = normalizeDate(p?.date);
    if (!d) continue;
    if (d >= todayYmd) continue;
    const close = typeof p.close === 'number' ? p.close : Number(p.close);
    if (!Number.isFinite(close)) continue;
    filtered.push({ date: d, close });
  }
  filtered.sort((a, b) => a.date.localeCompare(b.date));
  const tail = filtered.slice(Math.max(0, filtered.length - count));
  return tail.map((x) => x.close);
}

export function enrichIntradayWithRealtimeBiasBoll20(
  intradayPoints: StockPricePoint[],
  prevDailyCloses: number[],
  options?: {
    window?: number;
    prevRequired?: number;
    bollK?: number;
    ddof?: 0 | 1;
  },
): RealtimeIndicatorsEnrichResult {
  const window = options?.window ?? 20;
  const prevRequired = options?.prevRequired ?? 19;
  const bollK = options?.bollK ?? 2;
  const ddof = options?.ddof ?? 1;

  const today = pickTodayDateFromIntraday(intradayPoints) ?? undefined;

  if (window !== prevRequired + 1) {
    return {
      ok: false,
      points: intradayPoints,
      reason: 'window 与 prevRequired 不匹配',
      prevDaysUsed: Math.min(prevDailyCloses.length, prevRequired),
      today,
    };
  }

  if (prevDailyCloses.length < prevRequired) {
    return {
      ok: false,
      points: intradayPoints,
      reason: '历史日线不足',
      prevDaysUsed: prevDailyCloses.length,
      today,
    };
  }

  const prev = prevDailyCloses.slice(prevDailyCloses.length - prevRequired);

  let sumPrev = 0;
  let sumSqPrev = 0;
  for (const c of prev) {
    sumPrev += c;
    sumSqPrev += c * c;
  }

  const n = window;
  const denom = ddof === 1 ? n - 1 : n;

  const points = intradayPoints.map((p) => {
    const close = typeof p.close === 'number' ? p.close : Number(p.close);
    if (!Number.isFinite(close)) return p;

    const sum = sumPrev + close;
    const mean = sum / n;
    const sumSq = sumSqPrev + close * close;

    let variance = 0;
    if (denom > 0) {
      const centered = sumSq - (sum * sum) / n;
      variance = centered / denom;
    }
    if (!Number.isFinite(variance) || variance < 0) variance = 0;
    const std = Math.sqrt(variance);

    const bollMid = mean;
    const bollUpper = mean + bollK * std;
    const bollLower = mean - bollK * std;
    const bias20 = mean !== 0 ? ((close - mean) / mean) * 100 : undefined;

    return {
      ...p,
      boll_mid: Number.isFinite(bollMid) ? bollMid : undefined,
      boll_upper: Number.isFinite(bollUpper) ? bollUpper : undefined,
      boll_lower: Number.isFinite(bollLower) ? bollLower : undefined,
      bias20: typeof bias20 === 'number' && Number.isFinite(bias20) ? bias20 : undefined,
    };
  });

  return { ok: true, points, prevDaysUsed: prevRequired, today: today ?? '' };
}

