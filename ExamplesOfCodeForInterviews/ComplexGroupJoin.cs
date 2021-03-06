using System;
using System.Collections.Generic;
using System.Text;

namespace ExamplesOfCodeForInterviews
{
    class ComplexGroupJoin
    {
        public void GroupJoinExample()
        {
            var LLFCs = new List<List<LLFC>>();

            var meteredDataList = new List<MeterHourlyData>();
            var reactiveImportDataList = new List<MeterHourlyData>();
            var reactiveExportDataList = new List<MeterHourlyData>();
            var energyPriceDataList = new List<MeterHourlyData>();

            foreach (var mpan in mpans)
            {
                var llfcs = _context.LLFCs.Where(l => l.LLFCNumber == mpan.LLFC && l.ZoneId == mpan.ZoneId).ToList();
                LLFCs.Add(llfcs);

                var meteredDataForSingleMpan = _context.MeterDataRecords
                       .Where(m => m.MpanId == mpan.Id && m.SettlementDate >= startDate && m.SettlementDate < endDate)
                       .Select(m => new MeterHourlyData(m.SettlementDate, m.Period, m.ReferenceValue / 1000, m.Import, m.MpanId)).ToList();

                meteredDataList.AddRange(meteredDataForSingleMpan);

                var reactiveImportDataForSingleMpan = _context.ReactiveDataRecords
                   .Where(r => r.Import.HasValue && r.Import.Value && r.MpanId == mpan.Id && r.SettlementDate >= startDate && r.SettlementDate < endDate)
                   .Select(m => new MeterHourlyData(m.SettlementDate, m.Period, m.ReferenceValue, m.Import, m.MpanId))
                   .ToList();

                reactiveImportDataList.AddRange(reactiveImportDataForSingleMpan);

                var reactiveExportDataForSingleMpan = _context.ReactiveDataRecords
                   .Where(r => r.Import.HasValue && !r.Import.Value && r.MpanId == mpan.Id && r.SettlementDate >= startDate && r.SettlementDate < endDate)
                   .Select(m => new MeterHourlyData(m.SettlementDate, m.Period, m.ReferenceValue, m.Import, m.MpanId))
                   .ToList();

                reactiveExportDataList.AddRange(reactiveExportDataForSingleMpan);

                var energyPriceDataForSingleMpan = _context.EnergyPriceDataRecords
                    .Where(t => t.SettlementDate >= startDate && t.SettlementDate < endDate && t.MpanId == mpan.Id)
                    .Select(m => new MeterHourlyData(m.SettlementDate, m.Period, m.ReferenceValue, null, m.MpanId))
                    .ToList();

                energyPriceDataList.AddRange(energyPriceDataForSingleMpan);
            }

            List<Period> periods = _context.Periods.Where(p => p.TimePropertiesType == TimePropertiesType.LLFsProperties && p.ZoneId == invoiceDetails.MPAN.ZoneId && p.Year == startDate.AddMonths(-3).Year)
                .Include(p => p.DatePeriods)
                .Include(p => p.TimePeriods)
                .Include(p => p.Dates).ToList();

            var tradingUnits = _context.TradingUnitVolumesDataRecords
                        .Where(t => t.SettlementDate >= startDate && t.SettlementDate < endDate && t.ZoneId == invoiceDetails.MPAN.ZoneId)
                        .Select(m => new HourlyData(m.SettlementDate, m.Period, m.Volume, null)).ToList();

            var minMeterData = _context.MeterDataRecords.Where(m => m.MpanId == invoiceDetails.MPANId).Select(m => m.SettlementDate).Min();
            var tlmZoneTime = new DateTime(2018, 4, 1, 0, 0, 0, DateTimeKind.Utc);
            var tlmData = _context.TlmDataRecords
                        .Where(tlm => tlm.SettlementDate >= startDate && tlm.SettlementDate < endDate && tlm.SettlementDate >= minMeterData
                            && (tlm.SettlementDate < tlmZoneTime || (tlm.SettlementDate >= tlmZoneTime && tlm.Zone == invoiceDetails.MPAN.ZoneId)))
                        .Select(m => new HourlyData(
                            m.SettlementDate,
                            m.Period,
                            (tradingUnits.FirstOrDefault(t => t.Date == m.SettlementDate && t.Period == m.Period).Value ?? 0) <= 0 ? m.OffTaking : m.Delivering,
                            null)).ToList();

            var sbpSsp = _context.SspDataRecords.Where(e => e.SettlementDate < endDate && e.SettlementDate >= startDate)
                .Select(s => new HourlyData(s.SettlementDate, s.Period, s.SystemBuyPrice, null)).ToList();

            var n2exRecords = _context.N2EXRecords.Where(e => e.Date < endDate && e.Date >= startDate)
                .Select(n => new HourlyData(n.Date, n.Period, n.Value, null)).ToList();

            List<string> runTypesByPriority = new List<string> { "RF", "SF", "II" };
            var bSUoSDataAll = _context.SFRFDataRecords
                    .Where(m => m.SettlementDate >= startDate && m.SettlementDate < endDate)
                    .Select(m => new BsuosHourlyData(m.SettlementDate, m.Period, m.ReferenceValue, m.RunType)).ToList();

            var bSUoSData = new List<HourlyData>();
            foreach (var item in bSUoSDataAll.GroupBy(g => new { g.Date, g.Period }))
            {
                var result = item.FirstOrDefault(i => i.RunType == runTypesByPriority[0]) ??
                    item.FirstOrDefault(i => i.RunType == runTypesByPriority[1]) ??
                    item.FirstOrDefault(i => i.RunType == runTypesByPriority[2]);

                bSUoSData.Add(new HourlyData(item.Key.Date, item.Key.Period, result?.Value ?? 0, null));
            }

            var rcrcData = _context.RcrcDataRecords
                .Where(m => m.SettlementDate >= startDate && m.SettlementDate < endDate)
                .Select(m => new HourlyData(m.SettlementDate, m.Period, m.ReferenceValue, null)).ToList();

            var meteredData = meteredDataList.GroupBy(g => new { g.Date, g.Period }).Select(g => new MultipleMpansHourlyData(g.Key.Date, g.Key.Period, null, null, g.ToDictionary(i => i.MPANId, i => i.Value ?? 0))).ToList();
            var reactiveImportData = reactiveImportDataList.GroupBy(g => new { g.Date, g.Period }).Select(g => new MultipleMpansHourlyData(g.Key.Date, g.Key.Period, null, null, g.ToDictionary(i => i.MPANId, i => i.Value ?? 0))).ToList();
            var reactiveExportData = reactiveExportDataList.GroupBy(g => new { g.Date, g.Period }).Select(g => new MultipleMpansHourlyData(g.Key.Date, g.Key.Period, null, null, g.ToDictionary(i => i.MPANId, i => i.Value ?? 0))).ToList();
            var energyPriceData = energyPriceDataList.GroupBy(g => new { g.Date, g.Period }).Select(g => new MultipleMpansHourlyData(g.Key.Date, g.Key.Period, null, null, g.ToDictionary(i => i.MPANId, i => i.Value ?? 0))).ToList();

            var joinRecords = meteredData
                .GroupJoin(tlmData, m => new { m.Date, m.Period }, t => new { t.Date, t.Period }, (m, tlm) => new { Meter = m, Tlm = tlm })
                .SelectMany(meterTlm => meterTlm.Tlm.DefaultIfEmpty(), (join, tlm) =>
                    new
                    {
                        join.Meter.Date,
                        join.Meter.Period,
                        MeterValues = join.Meter.Values,
                        Tlm = tlm != null && tlm.Value.HasValue ? tlm.Value.Value : 0
                    })

                .GroupJoin(reactiveImportData, j => new { j.Date, j.Period }, r => new { r.Date, r.Period }, (j, reactive) => new { Join = j, Reactive = reactive })
                .SelectMany(joinReactive => joinReactive.Reactive.DefaultIfEmpty(), (join, reactive) =>
                    new
                    {
                        join.Join.Date,
                        join.Join.Period,
                        join.Join.MeterValues,
                        join.Join.Tlm,
                        ReactiveImportValues = reactive != null ? reactive.Values : new Dictionary<int, decimal>()
                    })

                .GroupJoin(reactiveExportData, j => new { j.Date, j.Period }, r => new { r.Date, r.Period }, (j, reactive) => new { Join = j, Reactive = reactive })
                .SelectMany(joinReactive => joinReactive.Reactive.DefaultIfEmpty(), (join, reactive) =>
                    new
                    {
                        join.Join.Date,
                        join.Join.Period,
                        join.Join.MeterValues,
                        join.Join.Tlm,
                        join.Join.ReactiveImportValues,
                        ReactiveExportValues = reactive != null ? reactive.Values : new Dictionary<int, decimal>()
                    })

                .GroupJoin(sbpSsp, j => new { j.Date, j.Period }, s => new { s.Date, s.Period }, (j, sbpSspList) => new { Join = j, SbpSspData = sbpSspList })
                .SelectMany(joinSbp => joinSbp.SbpSspData.DefaultIfEmpty(), (join, sbpSspItem) =>
                    new { join.Join.Date, join.Join.Period, join.Join.MeterValues, join.Join.Tlm, join.Join.ReactiveImportValues, join.Join.ReactiveExportValues, Sbp = (sbpSspItem != null && sbpSspItem.Value.HasValue) ? sbpSspItem.Value.Value : 0 })

                .GroupJoin(n2exRecords, j => new { j.Date, j.Period }, n => new { n.Date, n.Period }, (j, n2exList) => new { Join = j, N2exList = n2exList })
                .SelectMany(joinN2ex => joinN2ex.N2exList.DefaultIfEmpty(), (join, n2exItem) =>
                    new
                    {
                        join.Join.Date,
                        join.Join.Period,
                        join.Join.MeterValues,
                        join.Join.Tlm,
                        join.Join.ReactiveImportValues,
                        join.Join.ReactiveExportValues,
                        join.Join.Sbp,
                        N2Ex = (n2exItem != null && n2exItem.Value.HasValue) ? n2exItem.Value.Value : 0
                    })

                .GroupJoin(bSUoSData, j => new { j.Date, j.Period }, b => new { b.Date, b.Period }, (j, bSUoSList) => new { Join = j, BSUoSList = bSUoSList })
                .SelectMany(joinBsu => joinBsu.BSUoSList.DefaultIfEmpty(), (join, bsuItem) =>
                    new { join.Join.Date, join.Join.Period, join.Join.MeterValues, join.Join.Tlm, join.Join.ReactiveImportValues, join.Join.ReactiveExportValues, join.Join.Sbp, join.Join.N2Ex, Bsuos = (bsuItem != null && bsuItem.Value.HasValue) ? bsuItem.Value.Value : 0 })

                .GroupJoin(rcrcData, j => new { j.Date, j.Period }, r => new { r.Date, r.Period }, (j, rcrcList) => new { Join = j, RcrcList = rcrcList })
                .SelectMany(joinRcrc => joinRcrc.RcrcList.DefaultIfEmpty(), (join, rcrcItem) =>
                    new { join.Join.Date, join.Join.Period, join.Join.MeterValues, join.Join.Tlm, join.Join.ReactiveImportValues, join.Join.ReactiveExportValues, join.Join.Sbp, join.Join.N2Ex, join.Join.Bsuos, Rcrc = (rcrcItem != null && rcrcItem.Value.HasValue) ? rcrcItem.Value.Value : 0 })

                .GroupJoin(tradingUnits, j => new { j.Date, j.Period }, t => new { t.Date, t.Period }, (j, tradingUnitsList) => new { Join = j, TradingUnitsList = tradingUnitsList })
                .SelectMany(joinTradingUnits => joinTradingUnits.TradingUnitsList.DefaultIfEmpty(), (join, tradingUnitItem) =>
                    new
                    {
                        join.Join.Date,
                        join.Join.Period,
                        join.Join.MeterValues,
                        join.Join.Tlm,
                        join.Join.ReactiveImportValues,
                        join.Join.ReactiveExportValues,
                        join.Join.Sbp,
                        join.Join.N2Ex,
                        join.Join.Bsuos,
                        join.Join.Rcrc,
                        TradingUnit = (tradingUnitItem != null && tradingUnitItem.Value.HasValue) ? tradingUnitItem.Value.Value : 0
                    })

                .GroupJoin(energyPriceData, j => new { j.Date, j.Period }, t => new { t.Date, t.Period }, (j, energyPriceList) => new { Join = j, EnergyPriceList = energyPriceList })
                .SelectMany(joinEnergyPrice => joinEnergyPrice.EnergyPriceList.DefaultIfEmpty(), (join, energyPriceItem) =>
                    new
                    {
                        join.Join.Date,
                        join.Join.Period,
                        join.Join.MeterValues,
                        join.Join.Tlm,
                        join.Join.ReactiveImportValues,
                        join.Join.ReactiveExportValues,
                        join.Join.Sbp,
                        join.Join.N2Ex,
                        join.Join.Bsuos,
                        join.Join.Rcrc,
                        join.Join.TradingUnit,
                        EnergyPriceValues = energyPriceItem != null ? energyPriceItem.Values : new Dictionary<int, decimal>()
                    })
                .ToList();
        }
    }
}
