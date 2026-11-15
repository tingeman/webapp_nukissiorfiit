/* Phase 2 page: the ground temperature profile (layout.py, "profile-*"; docs/plan/PLAN_api-v2.md,
   T10, T16).

   dccFunctions.nukiDay: the tooltip of the day slider. The slider's value is the day as days
   since 1970-01-01 (UTC); Dash calls this function, named in the slider's tooltip.transform,
   to show it as YYYY-MM-DD.

   dash_clientside.nuki.profile: the clientside callback (callbacks.py) that draws the
   profile of the slider's day while it is dragged, without a request. Everything it uses is
   in the "profile" store, made by profile.store_data and figures.profile_spec: per depth
   (row) and day the mean, min, max and the annual envelope (amax, amin), per day the valid
   days of the envelope's window (and the station, Mast 9), and in store.draw the figure
   layout, the trace styles, which traces each envelope choice shows, the day marker and the
   label templates, and in store.source the mast and logger the profile is from (it starts
   the label). This function only picks the day's values. Returns [profile figure,
   label, Patch of the data figure's shapes (the day marker, plus the logger change lines)]. */
window.dccFunctions = window.dccFunctions || {};
window.dccFunctions.nukiDay = function (value) {
  if (value === null || value === undefined || isNaN(value)) { return ""; }
  return new Date(Number(value) * 86400000).toISOString().slice(0, 10);
};

window.dash_clientside = window.dash_clientside || {};
window.dash_clientside.nuki = Object.assign(window.dash_clientside.nuki || {}, {
  profile: function (dayValue, envelope, store) {
    var dc = window.dash_clientside;
    if (!store || !store.keys || !store.keys.length || !store.draw) {
      return [dc.no_update, dc.no_update, dc.no_update];
    }
    var draw = store.draw, last = store.first + store.n - 1;
    var day = (dayValue === null || dayValue === undefined || isNaN(dayValue)) ? last : Math.round(Number(dayValue));
    day = Math.min(Math.max(day, store.first), last);
    var i = day - store.first;
    var iso = window.dccFunctions.nukiDay;
    var field = new RegExp("\\{(\\w+)\\}", "g");     // str.format fields of texts.py
    var fmt = function (t, v) { return t.replace(field, function (m, k) { return k in v ? v[k] : m; }); };
    var station = store.day_station ? store.station_list[store.day_station[i]] : null;
    var rows = [];
    for (var r = 0; r < store.keys.length; r++) {
      if (station === null || store.stations[r] === station) { rows.push(r); }
    }
    var has = function (name) { return rows.filter(function (r) { return store[name][r][i] !== null; }); };
    var at = function (name, rs) { return rs.map(function (r) { return store[name][r][i]; }); };
    var depth = function (rs) { return rs.map(function (r) { return -store.depths[r]; }); };
    var dayRows = has("mean"), envRows = has("amax");
    var kinds = draw.envelopes[envelope] || draw.envelopes.day;
    var data = [];
    kinds.forEach(function (k) {
      var rs = (k === "annual-max" || k === "annual-min") ? envRows : dayRows;
      if (!rs.length) { return; }
      var t = Object.assign({}, draw.traces[k]);
      if (k === "day-band") {             // max down, then min back up
        t.x = at("max", rs).concat(at("min", rs).reverse());
        t.y = depth(rs).concat(depth(rs).reverse());
      } else if (k === "mean") {
        t.x = at("mean", rs); t.y = depth(rs);
        t.customdata = rs.map(function (r) { return [store.labels[r], store.min[r][i], store.max[r][i]]; });
      } else {
        t.x = at(k === "annual-max" ? "amax" : "amin", rs); t.y = depth(rs);
        t.customdata = rs.map(function (r) { return store.labels[r]; });
      }
      data.push(t);
    });
    var layout = Object.assign({}, draw.layout, {annotations: dayRows.length ? [] :
      [Object.assign({}, draw.no_day, {text: fmt(draw.no_day.text, {day: iso(day)})})]});
    var label = (store.source ? store.source + " · " : "") + fmt(draw.texts.label, {day: iso(day)});
    if (station !== null) { label += fmt(draw.texts.station, {station: station}); }
    if (kinds.indexOf("annual-max") >= 0) {
      var days = Math.min(store.window, i + 1);
      label += fmt(draw.texts.annual, {start: iso(day - days + 1), end: iso(day), valid: store.valid[i], days: days});
      if (days < store.window) { label += fmt(draw.texts.short, {start: iso(store.first)}); }
    }
    var marker = Object.assign({}, draw.marker, {x0: iso(day) + " 00:00:00", x1: iso(day + 1) + " 00:00:00"});
    var patch = new dc.Patch().assign(["layout", "shapes"], [marker].concat(store.shapes || [])).build();
    return [{data: data, layout: layout}, label, patch];
  }
});
