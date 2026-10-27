/** Filter the complete returned catalogue without turning missing readings into missing platforms. */
export function filterPlatformCoverage(rows, search = '', status = 'all') {
  const term = String(search).trim().toLocaleLowerCase('en');
  return rows.filter(row => (status === 'all' || row.reading_status === status) &&
    (!term || [row.stop_name, row.stop_id, row.parent_station].some(value =>
      String(value ?? '').toLocaleLowerCase('en').includes(term))));
}
