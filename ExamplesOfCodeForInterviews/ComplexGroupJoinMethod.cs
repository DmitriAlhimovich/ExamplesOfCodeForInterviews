using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ExamplesOfCodeForInterviews
{
    class ComplexGroupJoinMethod
    {
        private  (DateTime Date, byte Period, decimal EafVolume, decimal MsmVolume, decimal RbmVolume, decimal N2ex, decimal Sbp, decimal Ssp, decimal Rcrc, decimal Apx, decimal Tlm, CelsaNominationData nomination)[]
            ApplyJoin(List<MeterData> eafData, List<MeterData> msmData, List<MeterData> rbmData, List<N2EX> n2exData,
            List<SspData> sspDataRecords, List<RcrcData> rcrcData, List<ApxData> apxData, List<HourlyData> tlmData, List<CelsaNominationData> nominations)
        {
            return eafData
               .GroupJoin(msmData, key1 => new { Date = key1.SettlementDate, key1.Period }, key2 => new { Date = key2.SettlementDate, key2.Period }, (firstItem, secondItems) => new { Eaf = firstItem, MSMs = secondItems })
               .SelectMany(joinItem => joinItem.MSMs.DefaultIfEmpty(), (join, secondItem) =>
                   new
                   {
                       Date = join.Eaf.SettlementDate,
                       join.Eaf.Period,
                       EafVolume = join.Eaf.ReferenceValue,
                       MsmVolume = secondItem?.ReferenceValue ?? 0
                   })
                   .GroupJoin(rbmData, key1 => new { key1.Date, key1.Period }, key2 => new { Date = key2.SettlementDate, key2.Period }, (firstItem, secondItems) => new { Join = firstItem, RBMs = secondItems })
               .SelectMany(joinItem => joinItem.RBMs.DefaultIfEmpty(), (join, secondItem) =>
                   new
                   {
                       join.Join.Date,
                       join.Join.Period,
                       join.Join.EafVolume,
                       join.Join.MsmVolume,
                       RbmVolume = secondItem?.ReferenceValue ?? 0
                   })
                   .GroupJoin(n2exData, key1 => new { key1.Date, key1.Period }, key2 => new { key2.Date, key2.Period },
                   (firstItem, secondItems) => new { Join = firstItem, N2EXs = secondItems })
               .SelectMany(joinItem => joinItem.N2EXs.DefaultIfEmpty(), (join, secondItem) =>
                   new
                   {
                       join.Join.Date,
                       join.Join.Period,
                       join.Join.EafVolume,
                       join.Join.MsmVolume,
                       join.Join.RbmVolume,
                       N2ex = secondItem?.Value ?? 0
                   })
                   .GroupJoin(sspDataRecords, key1 => new { key1.Date, key1.Period }, key2 => new { Date = key2.SettlementDate, key2.Period },
                   (firstItem, secondItems) => new { Join = firstItem, SSPs = secondItems })
               .SelectMany(joinItem => joinItem.SSPs.DefaultIfEmpty(), (join, secondItem) =>
                   new
                   {
                       join.Join.Date,
                       join.Join.Period,
                       join.Join.EafVolume,
                       join.Join.MsmVolume,
                       join.Join.RbmVolume,
                       join.Join.N2ex,
                       Sbp = secondItem?.SystemBuyPrice ?? 0,
                       Ssp = secondItem?.SystemSellPrice ?? 0
                   })
                   .GroupJoin(rcrcData, key1 => new { key1.Date, key1.Period }, key2 => new { Date = key2.SettlementDate, key2.Period },
                        (firstItem, secondItems) => new { Join = firstItem, RCRCs = secondItems })
               .SelectMany(joinItem => joinItem.RCRCs.DefaultIfEmpty(), (join, secondItem) =>
                   new
                   {
                       join.Join.Date,
                       join.Join.Period,
                       join.Join.EafVolume,
                       join.Join.MsmVolume,
                       join.Join.RbmVolume,
                       join.Join.N2ex,
                       join.Join.Ssp,
                       join.Join.Sbp,
                       Rcrc = secondItem?.ReferenceValue ?? 0
                   })
                   .GroupJoin(apxData, key1 => new { key1.Date, key1.Period }, key2 => new { Date = key2.SettlementDate, key2.Period },
                        (firstItem, secondItems) => new { Join = firstItem, APXs = secondItems })
               .SelectMany(joinItem => joinItem.APXs.DefaultIfEmpty(), (join, secondItem) =>
                   new
                   {
                       join.Join.Date,
                       join.Join.Period,
                       join.Join.EafVolume,
                       join.Join.MsmVolume,
                       join.Join.RbmVolume,
                       join.Join.N2ex,
                       join.Join.Ssp,
                       join.Join.Sbp,
                       join.Join.Rcrc,
                       Apx = secondItem?.ReferenceValue ?? 0
                   })
                   .GroupJoin(tlmData, key1 => new { key1.Date, key1.Period }, key2 => new { key2.Date, key2.Period },
                        (firstItem, secondItems) => new { Join = firstItem, TLMs = secondItems })
               .SelectMany(joinItem => joinItem.TLMs.DefaultIfEmpty(), (join, secondItem) =>
               new
               {
                   join.Join.Date,
                   join.Join.Period,
                   join.Join.EafVolume,
                   join.Join.MsmVolume,
                   join.Join.RbmVolume,
                   join.Join.N2ex,
                   join.Join.Ssp,
                   join.Join.Sbp,
                   join.Join.Rcrc,
                   join.Join.Apx,
                   Tlm = secondItem?.Value ?? 0
               })
                   .GroupJoin(nominations, key1 => new { key1.Date, key1.Period }, key2 => new { Date = key2.SettlementDate, key2.Period },
                        (firstItem, secondItems) => new { Join = firstItem, Nominations = secondItems })
               .SelectMany(joinItem => joinItem.Nominations.DefaultIfEmpty(), (join, secondItem) =>

                   (
                       Date: join.Join.Date,
                       Period: join.Join.Period,
                       EafVolume: join.Join.EafVolume,
                       MsmVolume: join.Join.MsmVolume,
                       RbmVolume: join.Join.RbmVolume,
                       N2ex: join.Join.N2ex,
                       Sbp: join.Join.Sbp,
                       Ssp: join.Join.Ssp,
                       Rcrc: join.Join.Rcrc,
                       Apx: join.Join.Apx,
                       Tlm: join.Join.Tlm,
                       Nomination: secondItem
                   ))
                   .OrderBy(i => i.Date).ThenBy(i => i.Period)
                   .ToArray();
        }
    }

    internal class HourlyData
    {
        public DateTime Date { get; internal set; }
        public byte Period { get; internal set; }
        public decimal Value { get; internal set; }
    }

    internal class ApxData
    {
        public DateTime SettlementDate { get; internal set; }
        public byte Period { get; internal set; }
        public decimal ReferenceValue { get; internal set; }
    }

    internal class RcrcData
    {
        public DateTime SettlementDate { get; internal set; }
        public byte Period { get; internal set; }
        public decimal ReferenceValue { get; internal set; }
    }

    internal class SspData
    {
        public DateTime SettlementDate { get; internal set; }
        public byte Period { get; internal set; }
        public decimal SystemBuyPrice { get; internal set; }
        public decimal SystemSellPrice { get; internal set; }
    }

    internal class N2EX
    {
        public DateTime Date { get; internal set; }
        public byte Period { get; internal set; }
        public decimal Value { get; internal set; }
    }

    internal class MeterData
    {
        public DateTime SettlementDate { get; internal set; }
        public byte Period { get; internal set; }
        public decimal ReferenceValue { get; internal set; }
    }

    internal class CelsaNominationData
    {
        public DateTime SettlementDate { get; internal set; }
        public byte Period { get; internal set; }
    }
}
