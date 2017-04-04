from ...unit.import_synchronizer import BatchImportSynchronizer
from ...backend import prestashop

@prestashop
class PaymentMethodsImportSynchronizer(BatchImportSynchronizer):
    _model_name = 'payment.method'
    #TODO change to account.payment.mode or sale.order.status

    def run(self, filters=None, **kwargs):
        if filters is None:
            filters = {}
        filters['display'] = '[id,payment]'
        return super(PaymentMethodsImportSynchronizer, self).run(
            filters, **kwargs
        )

    def _import_record(self, record, **kwargs):
        ids = self.session.search('payment.method', [
            ('name', '=', record['payment']),
            ('company_id', '=', self.backend_record.company_id.id),
        ])
        if ids:
            return
        self.session.create('payment.method', {
            'name': record['payment'],
            'company_id': self.backend_record.company_id.id,
        })

    
    #def _import_record(self, record, **kwargs):
        """ Create the missing payment method

        If we have only 1 bank journal, we link the payment method to it,
        otherwise, the user will have to create manually the payment mode.
        """"""
        if self.binder_for().to_odoo(record['payment']):
            return  # already exists
        method_xmlid = 'account.account_payment_method_manual_in'
        payment_method = self.env.ref(method_xmlid, raise_if_not_found=False)
        if not payment_method:
            return
        journals = self.env['account.journal'].search(
            [('type', '=', 'bank'),
             ('company_id', '=', self.backend_record.company_id.id),
             ],
        )
        if len(journals) != 1:
            return
        mode = self.model.create({
            'name': record['payment'],
            'company_id': self.backend_record.company_id.id,
            'bank_account_link': 'fixed',
            'fixed_journal_id': journals.id,
            'payment_method_id': payment_method.id
        })
        self.backend_record.add_checkpoint(
            model=self.model._name,
            record_id=mode.id,
        )
        """
