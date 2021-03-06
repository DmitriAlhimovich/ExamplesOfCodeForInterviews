using System;
using System.Collections.Generic;
using System.Text;

namespace ExamplesOfCodeForInterviews.Refactoring
{
    class OldVersion
    {
        public void ChangeState(int id, InvoiceStatusType state, string jobId = "")
        {
            var invoice = _context.Invoices.FirstOrDefault(inv => inv.Id == id);
            var prevStatus = invoice.Status;
            if (invoice == null)
            {
                throw new ResponseException($"{_localizer["NotFound"]} ({nameof(Invoice)}: #{id})");
            }

            if (((state == InvoiceStatusType.Accepted || state == InvoiceStatusType.Rejected)
                    && invoice.Status != InvoiceStatusType.Generated)
                || (state == InvoiceStatusType.InProgress
                    && (invoice.Status == InvoiceStatusType.Accepted || invoice.Status == InvoiceStatusType.Rejected))
                || (state == InvoiceStatusType.CreditNote && invoice.Status != InvoiceStatusType.Accepted && invoice.Status != InvoiceStatusType.Sent))
            {
                throw new ResponseException($"{_localizer["NotAllowed"]}");
            }

            if (state == InvoiceStatusType.CreditNote)
            {
                var reconciliation = _context.Reconciliation.Where(
                    r => r.MpanId == invoice.MPANId
                    && r.InvoicingYear == invoice.StartDate.Year
                    && r.InvoicingMonth == invoice.StartDate.Month
                    && (!r.CreditNoteId.HasValue || r.CreditNoteId == 0))
                    .Include(r => r.ReconciliationValue).FirstOrDefault();
                if (reconciliation != null)
                {
                    reconciliation.CreditNoteId = invoice.Id;
                    _context.SaveChanges();
                }
            }

            if (state == InvoiceStatusType.Rejected)
            {
                var reconciliation = _context.Reconciliation.Where(
                    r => r.MpanId == invoice.MPANId
                    && r.InvoicingYear == invoice.StartDate.Year
                    && r.InvoicingMonth == invoice.StartDate.Month
                    && (!r.CreditNoteId.HasValue || r.CreditNoteId == 0))
                    .FirstOrDefault();
                _context.Reconciliation.Remove(reconciliation);
                _context.SaveChanges();
            }

            if (state == InvoiceStatusType.Sent && invoice.Status == InvoiceStatusType.CreditNote)
            {
                invoice.Status = InvoiceStatusType.SentCreditNote;
            }
            else
            {

                invoice.Status = state;
            }
            if (state == InvoiceStatusType.Accepted)
            {
                //invoice.InvoicingDate = DateTime.Now;

                // If invoice was accepted we need to regenerate excel and pdf files, it's need to change the Invoicing Date and number.
                GenerateResultsFiles(invoice, true);
            }

            if (state == InvoiceStatusType.CreditNote)
            {
                //TODO: edit invoice file for credite node
                invoice.InvoiceNumber = $"{(invoice.Status == InvoiceStatusType.CreditNote ? "CRN" : "INV")}{(invoice.InvoicingDate.HasValue ? invoice.InvoicingDate.Value : DateTime.Now).ToString("yyyyMMdd")}{(invoice.InvoiceNum > 0 ? invoice.InvoiceNum : invoice.MPANId.Value).ToString("D6")}";
                GenerateResultsFiles(invoice, true);
            }

            _context.DetachLocal(invoice, invoice.Id);
            invoice.LastUpdate = DateTime.UtcNow;
            invoice.JobId = jobId;
            if (invoice.PrevStatus != invoice.Status && prevStatus != InvoiceStatusType.InProgress)
            {
                invoice.PrevStatus = prevStatus;
            }
            _context.Invoices.Update(invoice);
            _context.SaveChanges();
        }

    }
}
