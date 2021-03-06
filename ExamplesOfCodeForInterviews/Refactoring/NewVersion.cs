using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace ExamplesOfCodeForInterviews.Refactoring
{
    class NewVersion
    {
        private readonly AUISContext _context;
        private object _configuration;

        public void ChangeState(int id, InvoiceStatusType state, string jobId = "")
        {
            var invoice = _context.Invoices.FirstOrDefault(inv => inv.Id == id);

            var prevStatus = invoice.Status;

            if (invoice == null)
                throw new ArgumentNullException(nameof(invoice));

            ValidateInvoiceStatusChange(state, invoice);

            invoice.Status = state == InvoiceStatusType.Sent && invoice.Status == InvoiceStatusType.CreditNote ? InvoiceStatusType.SentCreditNote : state;

            switch (state)
            {
                case InvoiceStatusType.Rejected:
                    ClearReconciliation(invoice);
                    break;
                case InvoiceStatusType.Accepted:
                    GenerateResultsFiles(invoice, true);
                    break;
                case InvoiceStatusType.CreditNote:
                    invoice.InvoiceNumber = $"{(invoice.Status == InvoiceStatusType.CreditNote ? "CRN" : "INV")}{(invoice.InvoicingDate.HasValue ? invoice.InvoicingDate.Value : DateTime.Now).ToString("yyyyMMdd")}{(invoice.InvoiceNum > 0 ? invoice.InvoiceNum : invoice.MPANId.Value).ToString("D6")}";
                    GenerateResultsFiles(invoice, true);
                    SetCreditNoteIdToReconciliation(invoice);
                    break;
            }
            
            invoice.LastUpdate = DateTime.UtcNow;
            invoice.JobId = jobId;
            if (invoice.PrevStatus != invoice.Status && prevStatus != InvoiceStatusType.InProgress)
            {
                invoice.PrevStatus = prevStatus;
            }
            _context.Invoices.Update(invoice);
            _context.SaveChanges();
        }

        private void GenerateResultsFiles(object invoice, bool v)
        {
            throw new NotImplementedException();
        }

        private void ValidateInvoiceStatusChange(InvoiceStatusType nextState, Invoice invoice)
        {
            if (AcceptOrRejectFromNotGeneratedStatus(invoice.Status, nextState)
                || RegenerateAcceptedOrRejectedStatus(invoice.Status, nextState)
                || CreditNoteFromNotAcceptedOrSent(invoice.Status, nextState))
            {
                throw new InvoiceWorkflowException($"Not allowed to change status '{invoice.Status.ToString()}' to '{nextState.ToString()}'");
            }
        }

        private static bool CreditNoteFromNotAcceptedOrSent(InvoiceStatusType currentStatus, InvoiceStatusType nextStatus)
            => nextStatus == InvoiceStatusType.CreditNote && currentStatus != InvoiceStatusType.Accepted && currentStatus != InvoiceStatusType.Sent;

        private bool RegenerateAcceptedOrRejectedStatus(InvoiceStatusType currentStatus, InvoiceStatusType nextStatus)
            => nextStatus == InvoiceStatusType.InProgress && (currentStatus == InvoiceStatusType.Accepted || currentStatus == InvoiceStatusType.Rejected);

        private bool AcceptOrRejectFromNotGeneratedStatus(InvoiceStatusType currentStatus, InvoiceStatusType nextStatus)
            => (nextStatus == InvoiceStatusType.Accepted || nextStatus == InvoiceStatusType.Rejected) && currentStatus != InvoiceStatusType.Generated;

        private void ClearReconciliation(Invoice invoice)
        {
            using (var context = new AUISContext(_configuration))
            {
                var reconciliations = context.Reconciliations.Where(
                    r => r.MpanId == invoice.MPANId
                    && r.InvoicingYear == invoice.StartDate.Year
                    && r.InvoicingMonth == invoice.StartDate.Month
                    && (!r.CreditNoteId.HasValue || r.CreditNoteId == 0));

                context.Reconciliations.RemoveRange(reconciliations);
                context.SaveChanges();
            }
        }

        private void SetCreditNoteIdToReconciliation(Invoice invoice)
        {
            using (var context = new AUISContext(_configuration))
            {
                var reconciliation = context.Reconciliations.FirstOrDefault(
                    r => r.MpanId == invoice.MPANId
                    && r.InvoicingYear == invoice.StartDate.Year
                    && r.InvoicingMonth == invoice.StartDate.Month
                    && (!r.CreditNoteId.HasValue || r.CreditNoteId == 0));

                if (reconciliation == null) return;

                reconciliation.CreditNoteId = invoice.Id;
                context.Reconciliations.Update(reconciliation);
                context.SaveChanges();
            }
        }

        #region stubs
        private class AUISContext : IDisposable
        {
            private object _configuration;

            public AUISContext(object configuration)
            {
                _configuration = configuration;
            }

            public IEnumerable<Invoice> Invoices { get; internal set; }
            public IEnumerable<Reconciliation> Reconciliations { get; internal set; }

            public void Dispose()
            {
                throw new NotImplementedException();
            }

            internal void SaveChanges()
            {
                throw new NotImplementedException();
            }
        }

        internal class Invoice
        {
            public DateTime StartDate { get; internal set; }
            public object MPANId { get; internal set; }
            public int Id { get; internal set; }
            public InvoiceStatusType Status { get; internal set; }
            public DateTime? InvoicingDate { get; internal set; }
            public InvoiceStatusType PrevStatus { get; internal set; }
            public DateTime LastUpdate { get; internal set; }
            public string JobId { get; internal set; }
        }

        public class Reconciliation
        {
            public int InvoicingYear { get; internal set; }
            public int InvoicingMonth { get; internal set; }
            public int? CreditNoteId { get; internal set; }
            public object MpanId { get; internal set; }
        }

        [Serializable]
        internal class InvoiceWorkflowException : Exception
        {
            public InvoiceWorkflowException()
            {
            }

            public InvoiceWorkflowException(string message) : base(message)
            {
            }

            public InvoiceWorkflowException(string message, Exception innerException) : base(message, innerException)
            {
            }

            protected InvoiceWorkflowException(SerializationInfo info, StreamingContext context) : base(info, context)
            {
            }
        }

        public enum InvoiceStatusType
        {
            New = -2,

            Error = -1,

            InProgress = 0,

            Generated = 1,

            Accepted = 2,

            Rejected = 3,

            CreditNote = 4,

            Sent = 5,

            SentCreditNote = 6,

            Reversed = 7

        }

        #endregion
    }

   
}
