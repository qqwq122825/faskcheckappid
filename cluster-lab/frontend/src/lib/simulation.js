/** Local presentation fixture. No input records, network calls or authentication. */
export const DEMO_TOTAL = 10000

/** @param {number} completed */
export function simulatedCounts(completed) {
  if (!Number.isInteger(completed) || completed < 0 || completed > DEMO_TOTAL) throw new RangeError('Invalid demo progress')
  const blocks = Math.floor(completed / 100), remainder = completed % 100
  return {
    success: blocks * 70 + Math.min(remainder, 70),
    failed: blocks * 12 + Math.min(Math.max(remainder - 70, 0), 12),
    timeout: blocks * 8 + Math.min(Math.max(remainder - 82, 0), 8),
    skipped: blocks * 10 + Math.max(remainder - 90, 0),
  }
}
