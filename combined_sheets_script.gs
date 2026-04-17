// ============================================================
// GreenClaw + KoiScale + KoiRyu + RossWatcher — Google Apps Script
// ============================================================
// SETUP:
// 1. Open your existing "GreenClaw Trading Log" spreadsheet
// 2. Extensions > Apps Script > REPLACE the existing script with this file > Save
// 3. Deploy > Manage deployments > Edit (pencil icon) > Version: New version > Deploy
//    (This updates the SAME webhook URL — no env var changes needed)
// ============================================================

var SHEETS = {
  // GreenClaw tabs (unchanged)
  summary:    "Summary",
  trades:     "Trades",
  eod:        "Daily P&L",
  scan:       "Scan Log",
  ross:       "Ross Insights",
  // KoiScale tabs
  koi_trades: "KoiScale Trades",
  koi_eod:    "KoiScale Daily P&L",
  koi_summary:"KoiScale Summary",
  // KoiRyu tabs (new)
  ryu_trades:  "KoiRyu Trades",
  ryu_daily:   "KoiRyu Daily",
  ryu_summary: "KoiRyu Summary"
};

// ── Entry point ──────────────────────────────────────────────
function doPost(e) {
  try {
    var data = JSON.parse(e.postData.contents);
    var type = data.type || "unknown";
    var bot  = data.bot_version || "";
    var isKoi = bot.indexOf("KoiScale") !== -1;
    var isRyu = bot.indexOf("KoiRyu")   !== -1;

    if (isRyu) {
      // ── KoiRyu routing ────────────────────────────────────
      if      (type === "trade_entry")    { handleRyuTradeEntry(data); }
      else if (type === "trade_exit")     { handleRyuTradeExit(data); }
      else if (type === "daily_summary")  { handleRyuDaily(data); }
      else if (type === "weekly_digest")  { handleRyuWeekly(data); }
      updateRyuSummary();
    } else if (isKoi) {
      // ── KoiScale routing ──────────────────────────────────
      if      (type === "trade_close") { handleKoiTrade(data); }
      else if (type === "eod")         { handleKoiEOD(data); }
      updateKoiSummary();
    } else {
      // ── GreenClaw routing (unchanged) ─────────────────────
      if      (type === "trade")        { handleTrade(data); }
      else if (type === "eod")          { handleEOD(data); }
      else if (type === "scan")         { handleScan(data); }
      else if (type === "ross_insight") { handleRossInsight(data); }
      updateSummary();
    }

    return ContentService
      .createTextOutput(JSON.stringify({ ok: true, type: type, bot: bot }))
      .setMimeType(ContentService.MimeType.JSON);

  } catch (err) {
    return ContentService
      .createTextOutput(JSON.stringify({ ok: false, error: err.message }))
      .setMimeType(ContentService.MimeType.JSON);
  }
}

// ================================================================
//  KOIRYU HANDLERS
// ================================================================

// ── KoiRyu Trade Entry ───────────────────────────────────────
function handleRyuTradeEntry(d) {
  var sheet = getOrCreateSheet(SHEETS.ryu_trades, [
    "Date", "Time", "Symbol", "Side", "Entry $", "Stop $",
    "Shares", "Risk %", "Consol Score", "RS 3mo",
    "Regime", "Exit $", "P&L $", "R-Multiple",
    "Hold Days", "Exit Reason", "Bot Version"
  ]);

  sheet.appendRow([
    formatDate(d.timestamp),
    formatTime(d.timestamp),
    d.symbol       || "",
    "LONG",
    d.entry        || "",
    d.stop         || "",
    d.shares       || "",
    d.risk_pct     || "",
    d.consol_score || 0,
    d.rs_3mo       || "",
    d.regime       || "",
    "",   // exit — filled on trade_exit
    "",   // pnl
    "",   // r-multiple
    "",   // hold days
    "",   // exit reason
    d.bot_version  || ""
  ]);

  formatRyuTradesSheet(sheet, "entry");
}

// ── KoiRyu Trade Exit ────────────────────────────────────────
function handleRyuTradeExit(d) {
  var sheet = getOrCreateSheet(SHEETS.ryu_trades, [
    "Date", "Time", "Symbol", "Side", "Entry $", "Stop $",
    "Shares", "Risk %", "Consol Score", "RS 3mo",
    "Regime", "Exit $", "P&L $", "R-Multiple",
    "Hold Days", "Exit Reason", "Bot Version"
  ]);

  // Try to find the matching entry row and update it
  var updated = false;
  if (d.symbol && sheet.getLastRow() > 1) {
    var data = sheet.getRange(2, 1, sheet.getLastRow() - 1, 17).getValues();
    for (var i = data.length - 1; i >= 0; i--) {
      // Match symbol (col 3) where exit is blank (col 12)
      if (String(data[i][2]) === String(d.symbol) && data[i][11] === "") {
        var row = i + 2; // offset for header
        sheet.getRange(row, 12).setValue(d.exit       || "");
        sheet.getRange(row, 13).setValue(d.pnl        || 0);
        sheet.getRange(row, 14).setValue(d.r_multiple  || 0);
        sheet.getRange(row, 15).setValue(d.hold_days   || 0);
        sheet.getRange(row, 16).setValue(d.exit_reason || "");

        // Color the row by outcome
        var pnl = Number(d.pnl) || 0;
        var bg = "#ffffff";
        if      (pnl > 0)  { bg = "#b7e1cd"; }  // green
        else if (pnl < 0)  { bg = "#f4c7c3"; }  // red
        else                { bg = "#f8f9fa"; }  // grey (breakeven)
        sheet.getRange(row, 1, 1, 17).setBackground(bg);
        updated = true;
        break;
      }
    }
  }

  // If no matching entry found, append as a standalone exit row
  if (!updated) {
    sheet.appendRow([
      formatDate(d.timestamp),
      formatTime(d.timestamp),
      d.symbol       || "",
      "EXIT",
      d.entry        || "",
      "",
      d.shares       || "",
      "",
      d.consol_score || 0,
      "",
      "",
      d.exit         || "",
      d.pnl          || 0,
      d.r_multiple   || 0,
      d.hold_days    || 0,
      d.exit_reason  || "",
      d.bot_version  || ""
    ]);
    formatRyuTradesSheet(sheet, "exit");
  }
}

// ── KoiRyu Daily Summary ─────────────────────────────────────
function handleRyuDaily(d) {
  var sheet = getOrCreateSheet(SHEETS.ryu_daily, [
    "Date", "Regime", "Equity $", "Positions", "Candidates",
    "Total Unrealized $", "Consec Losses", "Portfolio Detail"
  ]);

  // Build portfolio detail string
  var portfolio = d.portfolio || [];
  var detail = portfolio.map(function(p) {
    return p.symbol + " " + (p.unrealized_pct >= 0 ? "+" : "") +
           p.unrealized_pct + "% ($" + p.unrealized + ")";
  }).join("; ") || "No positions";

  sheet.appendRow([
    d.date              || "",
    d.regime            || "",
    d.equity            || 0,
    d.positions         || 0,
    d.candidates        || 0,
    d.total_unrealized  || 0,
    d.consecutive_losses || 0,
    detail
  ]);

  // Color unrealized P&L cell
  var last = sheet.getLastRow();
  var unr  = Number(d.total_unrealized) || 0;
  var cell = sheet.getRange(last, 6);
  if      (unr > 0) { cell.setBackground("#b7e1cd"); }
  else if (unr < 0) { cell.setBackground("#f4c7c3"); }
  else              { cell.setBackground("#f8f9fa"); }

  [1, 2, 3, 4, 5, 6].forEach(function(c) { sheet.autoResizeColumn(c); });
}

// ── KoiRyu Weekly Digest ─────────────────────────────────────
function handleRyuWeekly(d) {
  var sheet = getOrCreateSheet(SHEETS.ryu_daily, [
    "Date", "Regime", "Equity $", "Positions", "Candidates",
    "Total Unrealized $", "Consec Losses", "Portfolio Detail"
  ]);

  // Append weekly digest as a highlighted row in the daily sheet
  var summary = "WEEKLY: " + (d.trades || 0) + " trades, " +
                "W:" + (d.wins || 0) + " L:" + (d.losses || 0) + ", " +
                "WR:" + (d.win_rate || 0) + "%, " +
                "P&L:$" + (d.total_pnl || 0) + ", " +
                "AvgR:" + (d.avg_r || 0) + ", " +
                "AvgHold:" + (d.avg_hold || 0) + "d";

  sheet.appendRow([
    d.date || "",
    "WEEKLY",
    "",
    "",
    "",
    d.total_pnl || 0,
    "",
    summary
  ]);

  var last = sheet.getLastRow();
  sheet.getRange(last, 1, 1, 8)
    .setBackground("#d9ead3")
    .setFontWeight("bold");
}

// ── KoiRyu Summary ───────────────────────────────────────────
function updateRyuSummary() {
  var ss    = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(SHEETS.ryu_summary);
  if (!sheet) {
    sheet = ss.insertSheet(SHEETS.ryu_summary);
  }
  sheet.clearContents();

  var stats = calcRyuStats();

  var dailySheet = ss.getSheetByName(SHEETS.ryu_daily);
  var totalDays  = 0;
  var greenDays  = 0;
  if (dailySheet && dailySheet.getLastRow() > 1) {
    var dailyData = dailySheet.getRange(
      2, 1, dailySheet.getLastRow() - 1, 8
    ).getValues();
    dailyData.forEach(function(row) {
      if (String(row[1]) !== "WEEKLY") {
        totalDays++;
        if (Number(row[5]) > 0) { greenDays++; }
      }
    });
  }
  var greenPct = totalDays > 0
    ? (greenDays / totalDays * 100).toFixed(1) + "%" : "--";

  var data = [
    ["KoiRyu Performance Summary", ""],
    ["Last updated", new Date().toLocaleString()],
    ["", ""],
    ["-- Paper Trading Stats --", ""],
    ["Total trades",       stats.totalTrades],
    ["Wins",               stats.wins],
    ["Losses",             stats.losses],
    ["Win rate",           stats.winRate + "%"],
    ["Avg R-Multiple",     stats.avgR],
    ["Total realized P&L", "$" + stats.totalPnl.toFixed(2)],
    ["Avg hold (days)",    stats.avgHold],
    ["", ""],
    ["-- Daily Stats --", ""],
    ["Scan days",          totalDays],
    ["Green days (unrlzd)", greenDays],
    ["Red days (unrlzd)",   totalDays - greenDays],
    ["Green day rate",     greenPct],
    ["", ""],
    ["-- Strategy --", ""],
    ["Type",               "Swing Breakout (Qullamaggie)"],
    ["Regime filter",      "SPY/QQQ/VIX/Breadth"],
    ["Position sizing",    "0.3-0.5% risk per trade"]
  ];

  sheet.getRange(1, 1, data.length, 2).setValues(data);
  sheet.getRange(1, 1, 1, 2).setFontWeight("bold").setFontSize(14);
  [4, 13, 19].forEach(function(r) {
    sheet.getRange(r, 1).setFontWeight("bold");
  });
  sheet.setColumnWidth(1, 260);
  sheet.setColumnWidth(2, 220);

  var wr     = parseFloat(stats.winRate);
  var wrCell = sheet.getRange(8, 2);
  if      (wr >= 55) { wrCell.setBackground("#b7e1cd"); }
  else if (wr >= 40) { wrCell.setBackground("#fce8b2"); }
  else               { wrCell.setBackground("#f4c7c3"); }
}

function calcRyuStats() {
  var ss    = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(SHEETS.ryu_trades);
  if (!sheet || sheet.getLastRow() < 2) {
    return { totalTrades: 0, wins: 0, losses: 0, winRate: "0.0",
             avgR: "0.0", totalPnl: 0, avgHold: "0.0" };
  }

  var data = sheet.getRange(
    2, 1, sheet.getLastRow() - 1, 17
  ).getValues();

  // Columns: Date(0) Time(1) Symbol(2) Side(3) Entry(4) Stop(5)
  //          Shares(6) Risk%(7) ConsolScore(8) RS3mo(9) Regime(10)
  //          Exit(11) P&L(12) R-Multiple(13) HoldDays(14) ExitReason(15) BotVer(16)
  var wins = 0, losses = 0, totalR = 0, totalPnl = 0, totalHold = 0, closed = 0;

  data.forEach(function(row) {
    var exitPrice = row[11];
    if (exitPrice === "" || exitPrice === null) { return; }  // still open
    closed++;
    var pnl = Number(row[12]) || 0;
    var r   = Number(row[13]) || 0;
    var hold = Number(row[14]) || 0;
    if (pnl > 0) { wins++; }
    else         { losses++; }
    totalPnl  += pnl;
    totalR    += r;
    totalHold += hold;
  });

  var winRate = closed > 0 ? (wins / closed * 100).toFixed(1) : "0.0";
  var avgR    = closed > 0 ? (totalR / closed).toFixed(2)     : "0.0";
  var avgHold = closed > 0 ? (totalHold / closed).toFixed(1)  : "0.0";

  return { totalTrades: closed, wins: wins, losses: losses, winRate: winRate,
           avgR: avgR, totalPnl: totalPnl, avgHold: avgHold };
}

// ── KoiRyu formatting ────────────────────────────────────────
function formatRyuTradesSheet(sheet, side) {
  var last = sheet.getLastRow();
  if (last < 2) { return; }
  if (side === "entry") {
    // Light blue for open entries
    sheet.getRange(last, 1, 1, 17).setBackground("#cfe2f3");
  } else {
    // Standalone exit row — color by P&L
    var pnl = Number(sheet.getRange(last, 13).getValue()) || 0;
    var bg = "#ffffff";
    if      (pnl > 0) { bg = "#b7e1cd"; }
    else if (pnl < 0) { bg = "#f4c7c3"; }
    else               { bg = "#f8f9fa"; }
    sheet.getRange(last, 1, 1, 17).setBackground(bg);
  }
  [1, 2, 3, 5, 6, 12, 16].forEach(function(c) { sheet.autoResizeColumn(c); });
}

// ================================================================
//  KOISCALE HANDLERS (unchanged)
// ================================================================

// ── KoiScale Trade handler ───────────────────────────────────
function handleKoiTrade(d) {
  var sheet = getOrCreateSheet(SHEETS.koi_trades, [
    "Date", "Time", "Symbol", "Outcome", "Entry $", "Stop $",
    "Target 1 $", "Target 2 $", "R:R", "VWAP $",
    "Confluence", "Choppy", "Bot Version"
  ]);

  sheet.appendRow([
    formatDate(d.timestamp),
    formatTime(d.timestamp),
    d.symbol       || "",
    d.outcome      || "",
    d.entry        || "",
    d.stop_1       || "",
    d.target_1     || "",
    d.target_2     || "",
    d.rr           || "",
    d.vwap         || "",
    d.confluence   || 0,
    d.choppy ? "Yes" : "No",
    d.bot_version  || ""
  ]);

  formatKoiTradesSheet(sheet);
}

// ── KoiScale EOD handler ─────────────────────────────────────
function handleKoiEOD(d) {
  var sheet = getOrCreateSheet(SHEETS.koi_eod, [
    "Date", "Day", "Outcome", "Trades", "P&L $", "Equity $",
    "Prev Equity $", "Candidates", "Choppy Mode", "Strategy",
    "Running P&L $"
  ]);

  var lastRow    = sheet.getLastRow();
  var runningPnl = 0;
  if (lastRow > 1) {
    var prevRunning = sheet.getRange(lastRow, 11).getValue();
    runningPnl = (Number(prevRunning) || 0) + (Number(d.pnl) || 0);
  } else {
    runningPnl = Number(d.pnl) || 0;
  }

  sheet.appendRow([
    d.date         || "",
    d.day_name     || "",
    d.outcome      || "",
    d.trades       || 0,
    d.pnl          || 0,
    d.equity       || 0,
    d.last_equity  || 0,
    d.candidates   || 0,
    d.choppy_mode ? "Yes" : "No",
    d.strategy     || "",
    runningPnl
  ]);

  formatKoiEODSheet(sheet);
}

// ── KoiScale Summary ─────────────────────────────────────────
function updateKoiSummary() {
  var ss    = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(SHEETS.koi_summary);
  if (!sheet) {
    sheet = ss.insertSheet(SHEETS.koi_summary);
  }
  sheet.clearContents();

  var stats     = calcKoiStats();
  var eodSheet  = ss.getSheetByName(SHEETS.koi_eod);
  var greenDays = eodSheet ? countGreenDays(eodSheet) : 0;
  var totalDays = eodSheet ? Math.max(eodSheet.getLastRow() - 1, 0) : 0;
  var greenPct  = totalDays > 0
    ? (greenDays / totalDays * 100).toFixed(1) + "%" : "--";

  var data = [
    ["KoiScale Performance Summary", ""],
    ["Last updated", new Date().toLocaleString()],
    ["", ""],
    ["-- Paper Trading Stats --", ""],
    ["Total trades",       stats.totalTrades],
    ["Wins",               stats.wins],
    ["Win rate",           stats.winRate + "%"],
    ["Avg R:R",            stats.avgRR],
    ["Total P&L",          "$" + stats.totalPnl.toFixed(2)],
    ["Current equity",     "$" + stats.equity.toFixed(2)],
    ["", ""],
    ["-- Daily Stats --", ""],
    ["Trading days",       totalDays],
    ["Green days",         greenDays],
    ["Red days",           totalDays - greenDays],
    ["Green day rate",     greenPct],
    ["", ""],
    ["-- Strategy --", ""],
    ["Type",               "VWAP Mean Reversion (Brian Shannon)"],
    ["Stage-2 filter",     "Yes"],
    ["Choppy mode",        "Adaptive sizing"]
  ];

  sheet.getRange(1, 1, data.length, 2).setValues(data);
  sheet.getRange(1, 1, 1, 2).setFontWeight("bold").setFontSize(14);
  [4, 12, 18].forEach(function(r) {
    sheet.getRange(r, 1).setFontWeight("bold");
  });
  sheet.setColumnWidth(1, 260);
  sheet.setColumnWidth(2, 220);

  var wr     = parseFloat(stats.winRate);
  var wrCell = sheet.getRange(7, 2);
  if      (wr >= 65) { wrCell.setBackground("#b7e1cd"); }
  else if (wr >= 50) { wrCell.setBackground("#fce8b2"); }
  else               { wrCell.setBackground("#f4c7c3"); }
}

function calcKoiStats() {
  var ss    = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(SHEETS.koi_trades);
  if (!sheet || sheet.getLastRow() < 2) {
    return { totalTrades: 0, wins: 0, winRate: "0.0",
             avgRR: "0.0", totalPnl: 0, equity: 0 };
  }

  var data = sheet.getRange(
    2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()
  ).getValues();

  var wins = 0, totalRR = 0;

  data.forEach(function(row) {
    var outcome = String(row[3]).toUpperCase();
    var rr      = Number(row[8]) || 0;
    if (outcome === "CLOSED") { wins++; }
    totalRR += rr;
  });

  var total   = data.length;
  var winRate = total > 0 ? (wins / total * 100).toFixed(1) : "0.0";
  var avgRR   = total > 0 ? (totalRR / total).toFixed(2)    : "0.0";

  var eodSheet = ss.getSheetByName(SHEETS.koi_eod);
  var equity = 0, totalPnl = 0;
  if (eodSheet && eodSheet.getLastRow() > 1) {
    var lastEodRow = eodSheet.getLastRow();
    equity   = Number(eodSheet.getRange(lastEodRow, 6).getValue()) || 0;
    totalPnl = Number(eodSheet.getRange(lastEodRow, 11).getValue()) || 0;
  }

  return { totalTrades: total, wins: wins, winRate: winRate,
           avgRR: avgRR, totalPnl: totalPnl, equity: equity };
}

// ── KoiScale formatting ──────────────────────────────────────
function formatKoiTradesSheet(sheet) {
  var last = sheet.getLastRow();
  if (last < 2) { return; }
  var outcome = String(sheet.getRange(last, 4).getValue()).toUpperCase();
  var bg = "#ffffff";
  if      (outcome === "CLOSED")    { bg = "#b7e1cd"; }
  else if (outcome === "STOPPED")   { bg = "#f4c7c3"; }
  else if (outcome === "STOPPED_BE"){ bg = "#f8f9fa"; }
  else if (outcome === "EOD")       { bg = "#fce8b2"; }
  sheet.getRange(last, 1, 1, sheet.getLastColumn()).setBackground(bg);
  [1, 2, 3, 4].forEach(function(c) { sheet.autoResizeColumn(c); });
}

function formatKoiEODSheet(sheet) {
  var last = sheet.getLastRow();
  if (last < 2) { return; }
  var pnl     = Number(sheet.getRange(last, 5).getValue());
  var pnlCell = sheet.getRange(last, 5);
  if      (pnl > 0) { pnlCell.setBackground("#b7e1cd"); }
  else if (pnl < 0) { pnlCell.setBackground("#f4c7c3"); }
  else              { pnlCell.setBackground("#f8f9fa"); }
  [1, 2, 3].forEach(function(c) { sheet.autoResizeColumn(c); });
}

// ================================================================
//  GREENCLAW HANDLERS (unchanged from original)
// ================================================================

// ── Trade handler ────────────────────────────────────────────
function handleTrade(d) {
  var sheet = getOrCreateSheet(SHEETS.trades, [
    "Date", "Time", "Symbol", "Qty", "Entry $", "Stop $", "Target $",
    "Risk/sh", "Total Risk $", "R:R", "Target Type", "Size",
    "Float", "Catalyst", "VWAP", "HOD", "Pole Ht", "B-Mode",
    "Equity Before", "Trade #", "Order ID", "Result $", "Win/Loss"
  ]);

  sheet.appendRow([
    d.date         || "",
    formatTime(d.timestamp),
    d.symbol       || "",
    d.qty          || "",
    d.entry        || "",
    d.stop         || "",
    d.target       || "",
    d.risk         || "",
    d.total_risk   || "",
    d.rr           || "",
    d.target_type  || "",
    d.size_label   || "",
    d.float        || "",
    String(d.catalyst || "").substring(0, 60),
    d.vwap         || "",
    d.hod          || "",
    d.pole_height  || "",
    d.b_mode ? "Yes" : "No",
    d.equity       || "",
    d.trade_num    || "",
    d.order_id     || "",
    "",
    ""
  ]);

  formatTradesSheet(sheet);
}

// ── EOD handler ──────────────────────────────────────────────
function handleEOD(d) {
  var sheet = getOrCreateSheet(SHEETS.eod, [
    "Date", "Day", "Outcome", "Trades", "P&L $", "Equity $",
    "Prev Equity $", "Candidates", "B-Mode", "Running P&L $",
    "Win Rate %", "Avg R:R"
  ]);

  var lastRow    = sheet.getLastRow();
  var runningPnl = 0;
  if (lastRow > 1) {
    var prevRunning = sheet.getRange(lastRow, 10).getValue();
    runningPnl = (Number(prevRunning) || 0) + (Number(d.pnl) || 0);
  } else {
    runningPnl = Number(d.pnl) || 0;
  }

  var stats = calcStats();

  sheet.appendRow([
    d.date        || "",
    d.day_name    || "",
    d.outcome     || "",
    d.trades      || 0,
    d.pnl         || 0,
    d.equity      || 0,
    d.last_equity || 0,
    d.candidates  || 0,
    d.b_mode ? "Yes" : "No",
    runningPnl,
    stats.winRate,
    stats.avgRR
  ]);

  formatEODSheet(sheet);
}

// ── Scan handler ─────────────────────────────────────────────
function handleScan(d) {
  var sheet = getOrCreateSheet(SHEETS.scan, [
    "Date", "Time", "Candidates", "B-Mode", "Equity $", "Symbols Found"
  ]);

  var watchlist = d.watchlist || [];
  var symbols   = watchlist.map(function(w) { return w.symbol; }).join(", ") || "None";

  sheet.appendRow([
    d.date       || "",
    formatTime(d.timestamp),
    d.candidates || 0,
    d.b_mode ? "Yes" : "No",
    d.equity     || 0,
    symbols
  ]);
}

// ── Ross Insights handler ─────────────────────────────────────
function handleRossInsight(d) {
  var sheet = getOrCreateSheet(SHEETS.ross, [
    "Date", "Day", "Video Title", "Video URL",
    "Result", "Market Condition",
    "Stocks Traded", "Setups Used",
    "What's Working", "What's NOT Working",
    "GreenClaw Relevance", "Next Session Outlook",
    "Quotable Insight", "Full Analysis", "Transcript Length"
  ]);

  var analysis = d.analysis || "";

  function extractSection(label, maxLen) {
    maxLen = maxLen || 2000;
    var marker = "**" + label + ":**";
    var start  = analysis.indexOf(marker);
    if (start === -1) { return ""; }
    start = start + marker.length;

    var searchFrom = start;
    var end = analysis.length;
    while (searchFrom < analysis.length) {
      var nextBold = analysis.indexOf("\n**", searchFrom);
      if (nextBold === -1) { break; }
      var lineEnd = analysis.indexOf("\n", nextBold + 1);
      if (lineEnd === -1) { lineEnd = analysis.length; }
      var line = analysis.substring(nextBold + 1, lineEnd);
      if (line.indexOf(":**") !== -1) {
        end = nextBold;
        break;
      }
      searchFrom = nextBold + 1;
    }
    return analysis.substring(start, end).trim().substring(0, maxLen);
  }

  function extractResult() {
    var marker = "**Result:**";
    var start  = analysis.indexOf(marker);
    if (start === -1) { return ""; }
    start = start + marker.length;
    var end = analysis.indexOf("\n", start);
    if (end === -1) { end = Math.min(start + 120, analysis.length); }
    return analysis.substring(start, end).trim();
  }

  sheet.appendRow([
    d.date                  || "",
    d.day_name              || "",
    d.video_title           || "",
    d.video_url             || "",
    extractResult(),
    extractSection("Market Condition"),
    extractSection("Stocks Traded"),
    extractSection("Setup Types Used Today"),
    extractSection("What's Working Right Now"),
    extractSection("What's NOT Working"),
    extractSection("GreenClaw Relevance"),
    extractSection("Market Condition for Next Session"),
    extractSection("Quotable Insight"),
    analysis.substring(0, 50000),
    d.transcript_length     || 0
  ]);

  var last = sheet.getLastRow();
  sheet.getRange(last, 1, 1, sheet.getLastColumn())
    .setBackground(last % 2 === 0 ? "#f8f9fa" : "#ffffff");
  sheet.getRange(last, 14).setWrap(true);
  [1, 2, 3, 5, 6].forEach(function(c) { sheet.autoResizeColumn(c); });
}

// ================================================================
//  GREENCLAW SUMMARY + STATS (unchanged)
// ================================================================

function updateSummary() {
  var ss    = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(SHEETS.summary);
  if (!sheet) {
    sheet = ss.insertSheet(SHEETS.summary);
    ss.setActiveSheet(sheet);
    ss.moveActiveSheet(1);
  }
  sheet.clearContents();

  var stats     = calcStats();
  var eodSheet  = ss.getSheetByName(SHEETS.eod);
  var greenDays = eodSheet ? countGreenDays(eodSheet) : 0;
  var totalDays = eodSheet ? Math.max(eodSheet.getLastRow() - 1, 0) : 0;
  var greenPct  = totalDays > 0
    ? (greenDays / totalDays * 100).toFixed(1) + "%" : "--";

  var readyStatus = (stats.totalTrades >= 100 && parseFloat(stats.winRate) >= 65)
    ? "READY FOR LIVE TRADING" : "Paper phase in progress";

  var data = [
    ["GreenClaw Performance Summary", ""],
    ["Last updated", new Date().toLocaleString()],
    ["", ""],
    ["-- Paper Trading Stats --", ""],
    ["Total trades",           stats.totalTrades],
    ["Wins",                   stats.wins],
    ["Win rate",               stats.winRate + "%"],
    ["Avg R:R",                stats.avgRR],
    ["Total P&L",              "$" + stats.totalPnl.toFixed(2)],
    ["Current equity",         "$" + stats.equity.toFixed(2)],
    ["", ""],
    ["-- Daily Stats --", ""],
    ["Trading days",           totalDays],
    ["Green days",             greenDays],
    ["Red days",               totalDays - greenDays],
    ["Green day rate",         greenPct],
    ["", ""],
    ["-- Progress to Live Trading --", ""],
    ["Target trades (paper)",  100],
    ["Completed",              stats.totalTrades],
    ["Remaining",              Math.max(0, 100 - stats.totalTrades)],
    ["Min win rate needed",    "65%"],
    ["Current win rate",       stats.winRate + "%"],
    ["Status",                 readyStatus]
  ];

  sheet.getRange(1, 1, data.length, 2).setValues(data);

  sheet.getRange(1, 1, 1, 2).setFontWeight("bold").setFontSize(14);
  [4, 12, 18].forEach(function(r) {
    sheet.getRange(r, 1).setFontWeight("bold");
  });
  sheet.setColumnWidth(1, 260);
  sheet.setColumnWidth(2, 180);

  var wr     = parseFloat(stats.winRate);
  var wrCell = sheet.getRange(7, 2);
  if      (wr >= 65) { wrCell.setBackground("#b7e1cd"); }
  else if (wr >= 50) { wrCell.setBackground("#fce8b2"); }
  else               { wrCell.setBackground("#f4c7c3"); }
}

function calcStats() {
  var ss    = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(SHEETS.trades);
  if (!sheet || sheet.getLastRow() < 2) {
    return { totalTrades: 0, wins: 0, winRate: "0.0",
             avgRR: "0.0", totalPnl: 0, equity: 0 };
  }

  var data = sheet.getRange(
    2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()
  ).getValues();

  var wins = 0, totalRR = 0, totalPnl = 0, equity = 0;

  data.forEach(function(row) {
    var result  = row[21];
    var winLoss = row[22];
    var rr      = Number(row[9])  || 0;
    var eq      = Number(row[18]) || 0;

    if (winLoss === "W") { wins++; }
    if (result !== "" && result !== null) {
      totalPnl += Number(result) || 0;
    }
    totalRR += rr;
    if (eq > equity) { equity = eq; }
  });

  var total   = data.length;
  var winRate = total > 0 ? (wins / total * 100).toFixed(1) : "0.0";
  var avgRR   = total > 0 ? (totalRR / total).toFixed(2)    : "0.0";

  return { totalTrades: total, wins: wins, winRate: winRate,
           avgRR: avgRR, totalPnl: totalPnl, equity: equity };
}

// ================================================================
//  SHARED HELPERS
// ================================================================

function countGreenDays(sheet) {
  if (sheet.getLastRow() < 2) { return 0; }
  var pnls  = sheet.getRange(2, 5, sheet.getLastRow() - 1, 1).getValues();
  var count = 0;
  pnls.forEach(function(r) {
    if (Number(r[0]) > 0) { count++; }
  });
  return count;
}

function getOrCreateSheet(name, headers) {
  var ss    = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(name);
  if (!sheet) {
    sheet = ss.insertSheet(name);
    sheet.appendRow(headers);
    sheet.getRange(1, 1, 1, headers.length)
      .setFontWeight("bold")
      .setBackground("#1a73e8")
      .setFontColor("#ffffff");
    sheet.setFrozenRows(1);
  }
  return sheet;
}

function formatTime(ts) {
  if (!ts) { return ""; }
  try   { return new Date(ts).toLocaleTimeString(); }
  catch (e) { return String(ts); }
}

function formatDate(ts) {
  if (!ts) { return ""; }
  try   { return new Date(ts).toLocaleDateString(); }
  catch (e) { return String(ts); }
}

function formatTradesSheet(sheet) {
  var last = sheet.getLastRow();
  if (last < 2) { return; }
  sheet.getRange(last, 1, 1, sheet.getLastColumn())
    .setBackground(last % 2 === 0 ? "#f8f9fa" : "#ffffff");
  [1, 2, 3, 11, 12].forEach(function(c) { sheet.autoResizeColumn(c); });
}

function formatEODSheet(sheet) {
  var last    = sheet.getLastRow();
  if (last < 2) { return; }
  var pnl     = Number(sheet.getRange(last, 5).getValue());
  var pnlCell = sheet.getRange(last, 5);
  if      (pnl > 0) { pnlCell.setBackground("#b7e1cd"); }
  else if (pnl < 0) { pnlCell.setBackground("#f4c7c3"); }
  else              { pnlCell.setBackground("#f8f9fa"); }
  [1, 2, 3].forEach(function(c) { sheet.autoResizeColumn(c); });
}
