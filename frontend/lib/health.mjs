/** Presentation only: current arithmetic and missing values remain untouched. */
const finite = value => typeof value === 'number' && Number.isFinite(value);
const nonnegative = value => finite(value) && value >= 0;
const validScore = value => finite(value) && value >= 0 && value <= 100;
const unavailable = () => ({ status: 'unavailable', score: null, level: 'unknown' });

function sourceInstant(value) {
  if (typeof value !== 'string') return null;
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.exec(value);
  if (!match) return null;
  const [, year, month, day, hour, minute, second] = match.map(Number);
  if (month < 1 || month > 12 || day < 1 || day > new Date(Date.UTC(year, month, 0)).getUTCDate() ||
      hour > 23 || minute > 59 || second > 59) return null;
  const instant = Date.parse(value);
  return Number.isFinite(instant) ? instant : null;
}

function readingAge(asOf, originalAge, receivedAt, monotonicNow, serverNowMs) {
  const instant = sourceInstant(asOf);
  if (instant === null || !nonnegative(originalAge) || !nonnegative(receivedAt) ||
      !nonnegative(monotonicNow) || monotonicNow < receivedAt) return null;
  let age = originalAge + (monotonicNow - receivedAt) / 1000;
  // Server-envelope time can reject a future/inconsistent source. Browser wall
  // time never grants a new allowance or makes an old score younger.
  if (finite(serverNowMs)) {
    if (instant > serverNowMs) return null;
    age = Math.max(age, (serverNowMs - instant) / 1000);
  }
  return age;
}

function retained(score, level, asOf, age, reason) {
  return { status: 'last_valid', score, level: typeof level === 'string' ? level : 'unknown',
    asOf, ageSeconds: age,
    reason: typeof reason === 'string' && reason ? reason.slice(0, 300) : 'Waiting for current health inputs.' };
}

export function healthPresentation(row, receivedAt, monotonicNow, serverNowMs = null) {
  const current = row || {};
  if (validScore(current.health_score)) {
    const age = readingAge(current.health_source_as_of, current.health_age_seconds, receivedAt, monotonicNow, serverNowMs);
    const fresh = current.health_freshness_seconds;
    const retention = current.health_retention_seconds;
    if (age !== null && finite(fresh) && fresh > 0 && age <= Math.min(fresh, 120)) {
      return { status: 'current', score: current.health_score, level: current.health_level || 'unknown' };
    }
    if (current.health_retention_eligible === true && age !== null && finite(retention) && retention > 0 && age <= Math.min(retention, 300)) {
      return retained(current.health_score, current.health_level, current.health_source_as_of, age);
    }
  }
  const previous = current.last_valid_health;
  if (!previous || !validScore(previous.score) || !finite(previous.retention_seconds) || previous.retention_seconds <= 0) return unavailable();
  const age = readingAge(previous.source_as_of, previous.age_seconds, receivedAt, monotonicNow, serverNowMs);
  return age !== null && age <= Math.min(previous.retention_seconds, 300)
    ? retained(previous.score, previous.level, previous.source_as_of, age, previous.reason) : unavailable();
}
