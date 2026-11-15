/* Phase 2 page: the tooltip of the profile's day slider (layout.py, "profile-day"). The
   slider's value is the day as days since 1970-01-01 (UTC); Dash calls this function, named
   in the slider's tooltip.transform, to show it as YYYY-MM-DD. */
window.dccFunctions = window.dccFunctions || {};
window.dccFunctions.nukiDay = function (value) {
  if (value === null || value === undefined || isNaN(value)) { return ""; }
  return new Date(Number(value) * 86400000).toISOString().slice(0, 10);
};
