using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AUIS.BLL.DTO;
using AUIS.BLL.Interfaces;
using AUIS.BLL.Utils;
using AUIS.Common.Interfaces;
using AUIS.DAL;
using AutoMapper;
using Microsoft.EntityFrameworkCore;
using NPOI.HSSF.Util;
using NPOI.SS.UserModel;
using NPOI.XSSF.UserModel;

namespace AUIS.BLL.Services
{
    public interface IConsolidationReportService
    {
        IEnumerable<ConsolidationReportItemDTO> GetConsolidationReport(DateTime startDate, DateTime endDate, string[] mPANIds, string[] invoicePointsNames, string[] dataTypesNames);
        Stream GenerateExcel(IEnumerable<ConsolidationReportItemDTO> invoiceSummaries);
        List<string> GetDatesTitles(IEnumerable<ConsolidationReportItemDTO> consolidationReportItems);
        Dictionary<string, List<string>> CollectAllInvoiceGroupsAndPointsNames(IEnumerable<ConsolidationReportItemDTO> consolidationReportItems);
    }
    public class ConsolidationReportService : IConsolidationReportService
    {
        private const string VolumeGroupName = "Volume";
        private const string TotalGroupName = "Total";
        private const string NbpVolumePointName = "NBP Volume";
        private const string MeteredVolumePointName = "Metered Volume";
        private readonly AUISContext _context;
        private readonly IInvoiceService _invoiceService;
        private readonly IFileStorage _fileStorage;
        private readonly IReportsService _reportsService;
        private readonly IMpanService _mpanService;
        private readonly IMapper _mapper;
        private readonly GroupsComparer groupComparer = new GroupsComparer();

        public ConsolidationReportService(AUISContext context, IInvoiceService invoiceService, IFileStorage fileStorage, IReportsService reportsService, IMpanService mpanService, IMapper mapper)
        {
            _context = context;
            _invoiceService = invoiceService;
            _fileStorage = fileStorage;
            _reportsService = reportsService;
            _mpanService = mpanService;
            _mapper = mapper;
        }

        public IEnumerable<ConsolidationReportItemDTO> GetConsolidationReport(DateTime startDate, DateTime endDate, string[] mpanValues, string[] invoicePointsNames, string[] dataTypesNames = null)
        {
            var consolidationReportList = new List<ConsolidationReportItemDTO>();

            for (var date = startDate; date <= endDate; date = date.AddMonths(1))
            {
                foreach (var mpanValue in mpanValues)
                {
                    var mpan = _mpanService.GetByName(mpanValue);                    

                    var consolidationReportItem = new ConsolidationReportItemDTO
                    {
                        MpanId = mpan.Id,
                        MpanName = mpan.MPANValue,
                        ContractId = mpan.ContractId ?? 0,
                        ContractName = mpan.Contract.Description,
                        SiteName = mpan.SiteName ?? mpan.Contract.SiteName ?? "-",
                        Year = date.Year,
                        Month = date.Month,
                        InvoicePointValues = new Dictionary<string, Dictionary<string, decimal>>()
                    };

                    consolidationReportList.Add(consolidationReportItem);

                    var invoiceDetails = FindInvoiceForMpan(mpan.Id, date);

                    if (invoiceDetails == null)
                        continue;

                    //Metered and NBP volumes
                    if (dataTypesNames != null && dataTypesNames.Length > 0)
                        consolidationReportItem.InvoicePointValues.Add(VolumeGroupName, new Dictionary<string, decimal>());

                    foreach (var dataTypeName in dataTypesNames ?? new string[] { })
                    {
                        Enum.TryParse(typeof(InvoiceSummaryDataType), dataTypeName.Replace(" ", ""), out object dataTypeObject);

                        InvoiceSummaryDataType dataType = (InvoiceSummaryDataType)dataTypeObject;

                        if (dataType == InvoiceSummaryDataType.MeteredVolume)
                            consolidationReportItem.InvoicePointValues[VolumeGroupName].Add(MeteredVolumePointName, invoiceDetails.MeteredVolume);
                        else if (dataType == InvoiceSummaryDataType.NBPVolume)
                            consolidationReportItem.InvoicePointValues[VolumeGroupName].Add(NbpVolumePointName, invoiceDetails.NBPVolume);
                    }

                    // Invoice Groups
                    foreach (var group in invoiceDetails.Groups)
                    {
                        var groupPoints = group.Points.OrderBy(p => p.PlaceIndex);

                        if (!consolidationReportItem.InvoicePointValues.Keys.Contains(group.Name))
                            consolidationReportItem.InvoicePointValues.Add(group.Name, new Dictionary<string, decimal>());

                        foreach (var point in groupPoints)
                        {
                            consolidationReportItem.InvoicePointValues[group.Name].Add(point.Name, point.SubAmount);
                        }
                    }
                }
            }

            //Total
            var vat = _context.InvoicePaymentSettings.SingleOrDefault(s => s.Id == _context.InvoiceTemplates.First().PaymentSettingId)?.Vat / 100 ?? 0;

            foreach (var item in consolidationReportList)
            {
                item.InvoicePointValues.Add(TotalGroupName, new Dictionary<string, decimal>());
                var allPointsSum = item.InvoicePointValues.Where(p => p.Key != VolumeGroupName).SelectMany(g => g.Value).Sum(p => p.Value);
                item.InvoicePointValues[TotalGroupName].Add("Net", allPointsSum);
                item.InvoicePointValues[TotalGroupName].Add("VAT", allPointsSum * vat);
                item.InvoicePointValues[TotalGroupName].Add("Total", allPointsSum * (1 + vat));
            }

            return consolidationReportList.OrderBy(i => i.ContractName).ThenBy(i => i.MpanName);
        }

        private InvoiceDetailsDTO FindInvoiceForMpan(int mpanId, DateTime month)
        {
            var invoice = _context.Invoices.AsNoTracking().FirstOrDefault(i => i.StartDate.Date == month.Date && i.MPANId == mpanId);

            if (invoice == null)
                return null;

            var invoiceDetails = _invoiceService.GetFullDetails(invoice.Id);

            return invoiceDetails;
        }

        public Stream GenerateExcel(IEnumerable<ConsolidationReportItemDTO> consolidationReportItems)
        {
            if (consolidationReportItems == null || consolidationReportItems.Count() == 0)
                throw new ArgumentNullException(nameof(consolidationReportItems));

            var excelFile = _fileStorage.CreateReportsResultPath() + Guid.NewGuid();

            using (var fsNew = new FileStream(excelFile, FileMode.Create, FileAccess.Write))
            {
                IWorkbook workbook;
                workbook = new XSSFWorkbook();

                var defStyle = workbook.CreateCellStyle(false, null, false, false, false, false);

                var allDates = GetAllDates(consolidationReportItems);

                var invoiceGroupPointsDictionary = CollectAllInvoiceGroupsAndPointsNames(consolidationReportItems);

                foreach (var date in allDates)
                {
                    ISheet excelSheet = workbook.CreateSheet(ConvertToDateTitle(date.Year, date.Month));

                    FillExcelHeader(workbook, excelSheet, defStyle, invoiceGroupPointsDictionary);

                    FillExcelData(workbook, excelSheet, defStyle, consolidationReportItems.Where(i => i.Year == date.Year && i.Month == date.Month), invoiceGroupPointsDictionary);
                }

                workbook.Write(fsNew);

                return _reportsService.GetFileStream(excelFile);
            }
        }

        private void FillExcelData(IWorkbook workbook, ISheet excelSheet, ICellStyle defStyle, IEnumerable<ConsolidationReportItemDTO> consolidationReportItems, Dictionary<string, List<string>> invoiceGroupPointsDictionary)
        {
            var numberDataFormat = workbook.CreateDataFormat().GetFormat("#,##0.00");
            defStyle.DataFormat = numberDataFormat;

            var styleLeftBordered = workbook.CreateCellStyle(false, null, true, false, false, false);
            styleLeftBordered.LeftBorderColor = HSSFColor.Black.Index;
            styleLeftBordered.DataFormat = numberDataFormat;

            var totalStyle = workbook.CreateCellStyle(true, null, true, false, true, false);
            totalStyle.TopBorderColor = HSSFColor.Black.Index;
            totalStyle.DataFormat = numberDataFormat;

            int currentRow = 2;
            int startCell = 0;

            int curCell = 0;

            foreach (var item in consolidationReportItems)
            {
                curCell = startCell;

                IRow row = excelSheet.GetOrCreateRow(currentRow++, defStyle);

                row.SetCellValue(curCell++, item.ContractName);
                row.SetCellValue(curCell++, item.MpanName);
                row.SetCellValue(curCell++, item.SiteName);

                foreach (var groupPair in invoiceGroupPointsDictionary)
                {
                    //var pointsValues = item.InvoicePointValues[groupPair.Key];
                    bool isFirst = true;
                    foreach (var pointName in invoiceGroupPointsDictionary[groupPair.Key])
                    {
                        row.SetCellValue(curCell++, GetValueByGroupAndPointKeysOrDefault(item, groupPair.Key, pointName), isFirst ? styleLeftBordered : defStyle);
                        if (isFirst)
                            isFirst = false;
                    }
                }
            }

            curCell = 2;
            IRow totalRow = excelSheet.GetOrCreateRow(currentRow++, defStyle);

            totalRow.SetCellValue(curCell++, "Total (by MPANs)", totalStyle);

            foreach (var groupKey in invoiceGroupPointsDictionary.Keys)
            {
                foreach (var pointKey in invoiceGroupPointsDictionary[groupKey])
                {
                    totalRow.SetCellValue(curCell++, consolidationReportItems.Sum(i => GetValueByGroupAndPointKeysOrDefault(i, groupKey, pointKey)), totalStyle);
                }
            }
        }

        private void FillExcelHeader(IWorkbook workbook, ISheet excelSheet, ICellStyle defStyle, Dictionary<string, List<string>> invoiceGroupPointsDictionary)
        {
            var headerStyle = workbook.CreateCellStyle(false, new XSSFColor(new byte[] { 217, 217, 217 }), false, false, false, false);
            headerStyle.WrapText = true;
            headerStyle.Alignment = HorizontalAlignment.Center;
            headerStyle.VerticalAlignment = VerticalAlignment.Center;

            var headerStyleLeftBordered = workbook.CreateCellStyle(false, new XSSFColor(new byte[] { 217, 217, 217 }), true, false, false, false);
            headerStyleLeftBordered.WrapText = true;
            headerStyleLeftBordered.Alignment = HorizontalAlignment.Center;
            headerStyleLeftBordered.VerticalAlignment = VerticalAlignment.Center;
            headerStyleLeftBordered.LeftBorderColor = HSSFColor.Black.Index;

            var groupHeaderStyle = workbook.CreateCellStyle(false, new XSSFColor(new byte[] { 50, 50, 50 }), true, true, false, false);
            groupHeaderStyle.WrapText = true;
            groupHeaderStyle.Alignment = HorizontalAlignment.Center;
            groupHeaderStyle.VerticalAlignment = VerticalAlignment.Center;
            groupHeaderStyle.LeftBorderColor = HSSFColor.Black.Index;
            IFont whiteFont = workbook.CreateFont();
            whiteFont.Color = HSSFColor.White.Index;
            groupHeaderStyle.SetFont(whiteFont);

            int firstRowIndex = 0;
            int startCell = 3;

            IRow firstRow = excelSheet.GetOrCreateRow(firstRowIndex, defStyle);
            IRow secondRow = excelSheet.GetOrCreateRow(firstRowIndex + 1, defStyle);

            firstRow.HeightInPoints = 26;
            secondRow.HeightInPoints = 26;

            firstRow.SetCellValue(0, "", groupHeaderStyle, cellSpan: 3);

            excelSheet.SetColumnWidth(0, 6000);
            secondRow.SetCellValue(0, "Contract", headerStyle);
            excelSheet.SetColumnWidth(1, 4000);
            secondRow.SetCellValue(1, "MPAN", headerStyle);
            excelSheet.SetColumnWidth(2, 4000);
            secondRow.SetCellValue(2, "Site name", headerStyle);

            int curCell = startCell;

            foreach (var pair in invoiceGroupPointsDictionary)
            {
                firstRow.SetCellValue(curCell, pair.Key, groupHeaderStyle, cellSpan: pair.Value.Count);

                bool isFirst = true;
                foreach (var pointName in pair.Value)
                {
                    excelSheet.SetColumnWidth(curCell, pair.Value.Count > 1 ? 4000 : 5000);
                    if (isFirst)
                    {
                        secondRow.SetCellValue(curCell++, pointName, headerStyleLeftBordered);
                        isFirst = false;
                    }
                    else
                        secondRow.SetCellValue(curCell++, pointName, headerStyle);
                }
            }
        }

        public List<string> GetDatesTitles(IEnumerable<ConsolidationReportItemDTO> consolidationReportItems)
        {
            return consolidationReportItems.Select(r => ConvertToDateTitle(r.Year, r.Month)).Distinct().ToList();
        }

        private static string ConvertToDateTitle(int year, int month)
        {
            return (month < 10 ? "0" : "") + $"{month}-{year}";
        }

        private IEnumerable<DateTime> GetAllDates(IEnumerable<ConsolidationReportItemDTO> consolidationReportItems)
        {
            return consolidationReportItems.Select(r => new DateTime(r.Year, r.Month, 1)).Distinct();
        }

        public Dictionary<string, List<string>> CollectAllInvoiceGroupsAndPointsNames(IEnumerable<ConsolidationReportItemDTO> consolidationReportItems)
        {
            Dictionary<string, List<string>> groupsPointsDictionary = new Dictionary<string, List<string>>();

            foreach (var reportItem in consolidationReportItems)
            {
                foreach (var groupName in reportItem.InvoicePointValues.Keys)
                {
                    if (!groupsPointsDictionary.ContainsKey(groupName))
                        groupsPointsDictionary.Add(groupName, new List<string>());

                    foreach (var pointName in reportItem.InvoicePointValues[groupName].Keys)
                    {
                        if (!groupsPointsDictionary[groupName].Contains(pointName))
                            groupsPointsDictionary[groupName].Add(pointName);
                    }
                }
            }

            var result = groupsPointsDictionary.OrderBy(g => g.Key, groupComparer);

            return result.ToDictionary(pair => pair.Key, pair => pair.Value);
        }

        private static decimal GetValueByGroupAndPointKeysOrDefault(ConsolidationReportItemDTO item, string groupKey, string pointKey)
        {
            if (!item.InvoicePointValues.ContainsKey(groupKey))
                return 0;

            if (!item.InvoicePointValues[groupKey].ContainsKey(pointKey))
                return 0;

            return item.InvoicePointValues[groupKey][pointKey];
        }

        private class GroupsComparer : IComparer<string>
        {
            public int Compare(string x, string y)
            {
                if (x == VolumeGroupName)
                    return -1;
                if (y == VolumeGroupName)
                    return 1;

                if (x == TotalGroupName)
                    return 1;
                else if (x.Contains("reconciliation", StringComparison.OrdinalIgnoreCase))
                    return 1;

                if (y == TotalGroupName)
                    return -1;
                else if (y.Contains("reconciliation", StringComparison.OrdinalIgnoreCase))
                    return -1;
                else return x.CompareTo(y);
            }
        }
    }
}
