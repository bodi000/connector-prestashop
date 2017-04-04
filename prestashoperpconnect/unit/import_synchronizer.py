# -*- coding: utf-8 -*-
##############################################################################
#
#    Prestashoperpconnect : OpenERP-PrestaShop connector
#    Copyright (C) 2013 Akretion (http://www.akretion.com/)
#    Copyright 2013 Camptocamp SA
#    @author: Guewen Baconnier
#    @author: Alexis de Lattre <alexis.delattre@akretion.com>
#    @author Sébastien BEAU <sebastien.beau@akretion.com>
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program. If not, see <http://www.gnu.org/licenses/>.
#
##############################################################################

import logging
from contextlib import closing, contextmanager

from datetime import datetime
from datetime import timedelta
from openerp.tools import DEFAULT_SERVER_DATETIME_FORMAT
from openerp.modules.registry import RegistryManager
from openerp.addons.connector.queue.job import job
from openerp.addons.connector.unit.synchronizer import ImportSynchronizer
from openerp.addons.connector.connector import ConnectorUnit, Binder
from openerp.addons.connector.session import ConnectorSession
from openerp.addons.connector.exception import (
    RetryableJobError,
    FailedJobError,
    NothingToDoJob,
)
from ..backend import prestashop
from ..connector import get_environment
from backend_adapter import GenericAdapter
from .exception import OrderImportRuleRetry
from backend_adapter import PrestaShopCRUDAdapter

from prestapyt import PrestaShopWebServiceError
from ..connector import add_checkpoint
from openerp.osv import fields, osv


_logger = logging.getLogger(__name__)

RETRY_ON_ADVISORY_LOCK = 1  # seconds
RETRY_WHEN_CONCURRENT_DETECTED = 1  # seconds


class PrestashopBaseImportSynchronizer(ImportSynchronizer):

    def _import_dependency(self, prestashop_id, binding_model,
                           importer_class=None, always=False,
                           **kwargs):
        """
        Import a dependency. The importer class is a subclass of
        ``PrestashopImporter``. A specific class can be defined.

        :param prestashop_id: id of the prestashop id to import
        :param binding_model: name of the binding model for the relation
        :type binding_model: str | unicode
        :param importer_cls: :py:class:`openerp.addons.connector.\
                                        connector.ConnectorUnit`
                             class or parent class to use for the export.
                             By default: PrestashopImporter
        :type importer_cls: :py:class:`openerp.addons.connector.\
                                       connector.MetaConnectorUnit`
        :param always: if True, the record is updated even if it already
                       exists,
                       it is still skipped if it has not been modified on
                       PrestaShop
        :type always: boolean
        :param kwargs: additional keyword arguments are passed to the importer
        """
        if not prestashop_id:
            return
        if importer_class is None:
            importer_class = PrestashopImportSynchronizer
        binder = self.binder_for(binding_model)
        if always or not binder.to_odoo(prestashop_id):
            importer = self.unit_for(importer_class, model=binding_model)
            importer.run(prestashop_id, **kwargs)

class PrestashopImportSynchronizer(ImportSynchronizer):
    """ Base importer for Prestashop """

    def __init__(self, environment):
        """
        :param environment: current environment (backend, session, ...)
        :type environment: :py:class:`connector.connector.Environment`
        """
        super(PrestashopImportSynchronizer, self).__init__(environment)
        self.prestashop_id = None
        self.prestashop_record = None

    def _get_prestashop_data(self):
        """ Return the raw prestashop data for ``self.prestashop_id`` """
        return self.backend_adapter.read(self.prestashop_id)

    def _has_to_skip(self):
        """ Return True if the import can be skipped """
        return False

    def _import_dependencies(self):
        """ Import the dependencies for the record"""
        return

    def _map_data(self):
        """ Returns an instance of
        :py:class:`~openerp.addons.connector.unit.mapper.MapRecord`

        """
        return self.mapper.map_record(self.prestashop_record)

    def _validate_data(self, data):
        """ Check if the values to import are correct

        Pro-actively check before the ``Model.create`` or
        ``Model.update`` if some fields are missing

        Raise `InvalidDataError`
        """
        return

    def _get_binding(self):
        """Return the openerp id from the prestashop id"""
        return self.binder.to_openerp(self.prestashop_id)

    def _context(self, **kwargs):
        return dict(self.session.context, connector_no_export=True, **kwargs)

    def _create_context(self):
        return {'connector_no_export': True}

    def _create_data(self, map_record):
        return map_record.values(for_create=True)

    def _update_data(self, map_record):
        return map_record.values()

    def _create(self, data, context=None):
        """ Create the OpenERP record """
        # special check on data before import
#        self._validate_data(data)
#        binding = self.model.with_context(
#            **self._create_context()
#        ).create(data)
#        _logger.debug(
#            '%d created from prestashop %s', binding, self.prestashop_id)
#        return binding
        if context is None:
            context = self._context()
        binding = self.model.create(
            self.session.cr,
            self.session.uid,
            data,
            context=context
        )
        _logger.debug('%s %d created from prestashop %s',
                      self.model._name, binding, self.prestashop_id)
        return erp_id

    def _update(self, binding, data, context=None):
        """ Update an OpenERP record """
        # special check on data before import
#        self._validate_data(data)
#        binding.with_context(connector_no_export=True).write(data)
#        _logger.debug(
#            '%d updated from prestashop %s', binding, self.prestashop_id)
        if context is None:
            context = self._context()
        self.model.write(self.session.cr,
                         self.session.uid,
                         binding,
                         data,
                         context=context)
        _logger.debug('%s %d updated from prestashop %s',
                      self.model._name, binding, self.prestashop_id)
        return

    def _before_import(self):
        """ Hook called before the import, when we have the PrestaShop
        data"""
        return

    def _after_import(self, binding):
        """ Hook called at the end of the import """
        return

    @contextmanager
    def do_in_new_connector_env(self, cr, model_name=None):
        """ Context manager that yields a new connector environment

        Using a new Odoo Environment thus a new PG transaction.

        This can be used to make a preemptive check in a new transaction,
        for instance to see if another transaction already made the work.
        """
        #with openerp.api.Environment.manage():
        registry = RegistryManager.get(cr.dbname)
        with closing(registry.cursor()) as cr:
            try:
                new_env = openerp.api.Environment(cr, self.env.uid, self.env.context)
                new_connector_session = ConnectorSession(cr, uid, context=context)
                connector_env = self.connector_env.create_environment(
                    self.backend_record.with_env(new_env),
                    new_connector_session,
                    model_name or self.model._name,
                    connector_env=self.connector_env
                )
                yield connector_env
            except:
                cr.rollback()
                raise
            else:
                # Despite what pylint says, this a perfectly valid
                # commit (in a new cursor). Disable the warning.
                cr.commit()  # pylint: disable=invalid-commit

    def _check_in_new_connector_env(self):
        with self.do_in_new_connector_env() as new_connector_env:
            # Even when we use an advisory lock, we may have
            # concurrent issues.
            # Explanation:
            # We import Partner A and B, both of them import a
            # partner category X.
            #
            # The squares represent the duration of the advisory
            # lock, the transactions starts and ends on the
            # beginnings and endings of the 'Import Partner'
            # blocks.
            # T1 and T2 are the transactions.
            #
            # ---Time--->
            # > T1 /------------------------\
            # > T1 | Import Partner A       |
            # > T1 \------------------------/
            # > T1        /-----------------\
            # > T1        | Imp. Category X |
            # > T1        \-----------------/
            #                     > T2 /------------------------\
            #                     > T2 | Import Partner B       |
            #                     > T2 \------------------------/
            #                     > T2        /-----------------\
            #                     > T2        | Imp. Category X |
            #                     > T2        \-----------------/
            #
            # As you can see, the locks for Category X do not
            # overlap, and the transaction T2 starts before the
            # commit of T1. So no lock prevents T2 to import the
            # category X and T2 does not see that T1 already
            # imported it.
            #
            # The workaround is to open a new DB transaction at the
            # beginning of each import (e.g. at the beginning of
            # "Imp. Category X") and to check if the record has been
            # imported meanwhile. If it has been imported, we raise
            # a Retryable error so T2 is rollbacked and retried
            # later (and the new T3 will be aware of the category X
            # from the its inception).
            binder = new_connector_env.get_connector_unit(Binder)
            if binder.to_openerp(self.prestashop_id):
                raise RetryableJobError(
                    'Concurrent error. The job will be retried later',
                    seconds=RETRY_WHEN_CONCURRENT_DETECTED,
                    ignore_retry=True
                )

    def run(self, prestashop_id, **kwargs):
        """ Run the synchronization

        :param prestashop_id: identifier of the record on Prestashop
        """
        self.prestashop_id = prestashop_id
        lock_name = 'import({}, {}, {}, {})'.format(
            self.backend_record._name,
            self.backend_record.id,
            self.model._name,
            self.prestashop_id,
        )
        # Keep a lock on this import until the transaction is committed
        #self.advisory_lock_or_retry(lock_name,
        #                            retry_seconds=RETRY_ON_ADVISORY_LOCK)
        if not self.prestashop_record:
            self.prestashop_record = self._get_prestashop_data()

        binding = self._get_binding()
        if not binding:
            self._check_in_new_connector_env()

        skip = self._has_to_skip()
        if skip:
            return skip

        # import the missing linked resources
        self._import_dependencies()

        self._import(binding, **kwargs)

    def _import(self, binding, **kwargs):
        """ Import the external record.

        Can be inherited to modify for instance the session
        (change current user, values in context, ...)

        """

        map_record = self._map_data()

        if binding:
            record = self._update_data(map_record)
        else:
            record = self._create_data(map_record)

        # special check on data before import
        self._validate_data(record)

        if binding:
            self._update(binding, record)
        else:
            binding = self._create(record)

        self.binder.bind(self.prestashop_id, binding)

        self._after_import(binding)

    def _check_dependency(self, ext_id, model_name):
        ext_id = int(ext_id)
        if not self.get_binder_for_model(model_name).to_openerp(ext_id):
            import_record(
                self.session,
                model_name,
                self.backend_record.id,
                ext_id
            )


class BatchImportSynchronizer(ImportSynchronizer):
    """ The role of a BatchImportSynchronizer is to search for a list of
    items to import, then it can either import them directly or delay
    the import of each item separately.
    """
    page_size = 1000

    def run(self, filters=None, **kwargs):
        """ Run the synchronization """
        if filters is None:
            filters = {}
        if 'limit' in filters:
            self._run_page(filters, **kwargs)
            return
        page_number = 0
        filters['limit'] = '%d,%d' % (
            page_number * self.page_size, self.page_size)
        record_ids = self._run_page(filters, **kwargs)
        while len(record_ids) == self.page_size:
            page_number += 1
            filters['limit'] = '%d,%d' % (
                page_number * self.page_size, self.page_size)
            record_ids = self._run_page(filters, **kwargs)

    def _run_page(self, filters, **kwargs):
        record_ids = self.backend_adapter.search(filters)

        for record_id in record_ids:
            self._import_record(record_id, **kwargs)
        return record_ids

    def _import_record(self, record):
        """ Import a record directly or delay the import of the record """
        raise NotImplementedError


@prestashop
class AddCheckpoint(ConnectorUnit):
    """ Add a connector.checkpoint on the underlying model
    (not the prestashop.* but the _inherits'ed model) """

    _model_name = []

    def run(self, binding_id):
        binding = self.session.browse(self.model._name,
                                      binding_id)
        record = binding.openerp_id
        add_checkpoint(self.session,
                       record._model._name,
                       record.id,
                       self.backend_record.id)


@prestashop
class DirectBatchImport(BatchImportSynchronizer):
    """ Import the PrestaShop Shop Groups + Shops

    They are imported directly because this is a rare and fast operation,
    performed from the UI.
    """
    _model_name = [
        'prestashop.shop.group',
        'prestashop.shop',
        'prestashop.account.tax.group',
        'prestashop.sale.order.state',
    ]

    def _import_record(self, record):
        """ Import the record directly """
        import_record(
            self.session,
            self.model._name,
            self.backend_record.id,
            record
        )


@prestashop
class DelayedBatchImport(BatchImportSynchronizer):
    """ Delay import of the records """
    _model_name = [
        'prestashop.res.partner.category',
        'prestashop.res.partner',
        'prestashop.address',
        'prestashop.product.category',
        'prestashop.product.product',
        'prestashop.sale.order',
        'prestashop.refund',
        'prestashop.supplier',
        'prestashop.product.supplierinfo',
        'prestashop.mail.message',
    ]

    def _import_record(self, record, **kwargs):
        """ Delay the import of the records"""
        import_record.delay(
            self.session,
            self.model._name,
            self.backend_record.id,
            record,
            **kwargs
        )


@prestashop
class ResPartnerRecordImport(PrestashopImportSynchronizer):
    _model_name = 'prestashop.res.partner'

    def _import_dependencies(self):
        groups = self.prestashop_record.get('associations', {})\
            .get('groups', {}).get('group', [])
        if not isinstance(groups, list):
            groups = [groups]
        for group in groups:
            self._check_dependency(group['id'],
                                   'prestashop.res.partner.category')

    def _after_import(self, erp_id):
        binder = self.get_binder_for_model(self._model_name)
        ps_id = binder.to_backend(erp_id)
        import_batch.delay(
            self.session,
            'prestashop.address',
            self.backend_record.id,
            filters={'filter[id_customer]': '[%d]' % (ps_id)},
            priority=10,
        )


@prestashop
class SimpleRecordImport(PrestashopImportSynchronizer):
    """ Import one simple record """
    _model_name = [
        'prestashop.shop.group',
        'prestashop.shop',
        'prestashop.address',
        'prestashop.account.tax.group',
    ]


@prestashop
class MrpBomImport(PrestashopImportSynchronizer):
    _model_name = [
        'prestashop.mrp.bom',
    ]

    def _import_dependencies(self):
        record = self.prestashop_record

        bundle = record.get('associations', {}).get('product_bundle', {})
        if 'products' not in bundle:
            return
        products = bundle['products']
        if not isinstance(products, list):
            products = [products]
        for product in products:
            self._check_dependency(product['id'], 'prestashop.product.product')


@prestashop
class CombinationMrpBomImport(MrpBomImport):
    _model_name = [
        'prestashop.combination.mrp.bom',
    ]

    def _get_prestashop_data(self):
        """ Return the raw prestashop data for ``self.prestashop_id`` """
        combination_adapter = self.get_connector_unit_for_model(
            GenericAdapter, 'prestashop.product.combination'
        )
        combination = combination_adapter.read(self.prestashop_id)
        return self.backend_adapter.read(combination['id_product'])

    def _validate_data(self, data):
        combination_binder = self.get_connector_unit_for_model(
            Binder, 'prestashop.product.combination'
        )
        product_id = combination_binder.to_openerp(
            self.prestashop_id, unwrap=True
        )
        data['product_id'] = product_id

    def _after_import(self, erp_id):
        combination_binder = self.get_connector_unit_for_model(
            Binder, 'prestashop.product.combination'
        )
        combination_id = combination_binder.to_openerp(self.prestashop_id)
        erp_id = self._get_openerp_id()
        self.session.write(
            'prestashop.product.combination',
            combination_id,
            {'prestashop_bundle_id': erp_id}
        )


@prestashop
class MailMessageRecordImport(PrestashopImportSynchronizer):
    """ Import one simple record """
    _model_name = 'prestashop.mail.message'

    def _import_dependencies(self):
        record = self.prestashop_record
        self._check_dependency(record['id_order'], 'prestashop.sale.order')
        if record['id_customer'] != '0':
            self._check_dependency(
                record['id_customer'], 'prestashop.res.partner'
            )

    def _has_to_skip(self):
        record = self.prestashop_record
        binder = self.get_binder_for_model('prestashop.sale.order')
        ps_so_id = binder.to_openerp(record['id_order'])
        return record['id_order'] == '0' or not ps_so_id


@prestashop
class SupplierRecordImport(PrestashopImportSynchronizer):
    """ Import one simple record """
    _model_name = 'prestashop.supplier'

    def _create(self, record):
        try:
            return super(SupplierRecordImport, self)._create(record)
        except ZeroDivisionError:
            del record['image']
            return super(SupplierRecordImport, self)._create(record)

    def _after_import(self, erp_id):
        binder = self.get_binder_for_model(self._model_name)
        ps_id = binder.to_backend(erp_id)
        import_batch(
            self.session,
            'prestashop.product.supplierinfo',
            self.backend_record.id,
            filters={'filter[id_supplier]': '%d' % ps_id},
            priority=10,
        )


@prestashop
class SupplierInfoImport(PrestashopImportSynchronizer):
    _model_name = 'prestashop.product.supplierinfo'

    def _import_dependencies(self):
        record = self.prestashop_record
        try:
            self._check_dependency(
                record['id_supplier'], 'prestashop.supplier'
            )
            self._check_dependency(
                record['id_product'], 'prestashop.product.product'
            )

            if record['id_product_attribute'] != '0':
                self._check_dependency(
                    record['id_product_attribute'],
                    'prestashop.product.combination'
                )
        except PrestaShopWebServiceError:
            raise NothingToDoJob('Error fetching a dependency')


@prestashop
class SaleImportRule(ConnectorUnit):
    _model_name = ['prestashop.sale.order']

    def _rule_always(self, record, method):
        """ Always import the order """
        return True

    def _rule_never(self, record, method):
        """ Never import the order """
        raise NothingToDoJob('Orders with payment method %s '
                             'are never imported.' %
                             record['payment']['method'])

    def _rule_paid(self, record, method):
        """ Import the order only if it has received a payment """
        if self._get_paid_amount(record) == 0.0:
            raise OrderImportRuleRetry('The order has not been paid.\n'
                                       'The import will be retried later.')

    def _get_paid_amount(self, record):
        payment_adapter = self.get_connector_unit_for_model(
            GenericAdapter,
            '__not_exist_prestashop.payment'
        )
        payment_ids = payment_adapter.search({
            'filter[order_reference]': record['reference']
        })
        paid_amount = 0.0
        for payment_id in payment_ids:
            payment = payment_adapter.read(payment_id)
            paid_amount += float(payment['amount'])
        return paid_amount

    _rules = {'always': _rule_always,
              'paid': _rule_paid,
              'authorized': _rule_paid,
              'never': _rule_never,
              }

    def check(self, record):
        """ Check whether the current sale order should be imported
        or not. It will actually use the payment method configuration
        and see if the chosen rule is fullfilled.

        :returns: True if the sale order should be imported
        :rtype: boolean
        """
        session = self.session
        payment_method = record['payment']
        method_ids = session.search('payment.method',
                                    [('name', '=', payment_method)])
        if not method_ids:
            raise FailedJobError(
                "The configuration is missing for the Payment Method '%s'.\n\n"
                "Resolution:\n"
                "- Go to 'Sales > Configuration > Sales > Customer Payment "
                "Method'\n"
                "- Create a new Payment Method with name '%s'\n"
                "-Eventually  link the Payment Method to an existing Workflow "
                "Process or create a new one." % (payment_method,
                                                  payment_method))
        method = session.browse('payment.method', method_ids[0])

        self._rule_global(record, method)
        self._rules[method.import_rule](self, record, method)

    def _rule_global(self, record, method):
        """ Rule always executed, whichever is the selected rule """
        order_id = record['id']
        max_days = method.days_before_cancel
        if not max_days:
            return
        if self._get_paid_amount(record) != 0.0:
            return
        fmt = '%Y-%m-%d %H:%M:%S'
        order_date = datetime.strptime(record['date_add'], fmt)
        if order_date + timedelta(days=max_days) < datetime.now():
            raise NothingToDoJob('Import of the order %s canceled '
                                 'because it has not been paid since %d '
                                 'days' % (order_id, max_days))


@prestashop
class SaleOrderImport(PrestashopImportSynchronizer):
    _model_name = ['prestashop.sale.order']

    def _import_dependencies(self):
        record = self.prestashop_record
        self._check_dependency(record['id_customer'], 'prestashop.res.partner')
        self._check_dependency(
            record['id_address_invoice'], 'prestashop.address'
        )
        self._check_dependency(
            record['id_address_delivery'], 'prestashop.address'
        )

        if record['id_carrier'] != '0':
            self._check_dependency(record['id_carrier'],
                                   'prestashop.delivery.carrier')

        orders = record['associations']\
            .get('order_rows', {})\
            .get('order_row', [])
        if isinstance(orders, dict):
            orders = [orders]
        for order in orders:
            try:
                self._check_dependency(order['product_id'],
                                       'prestashop.product.product')
            except PrestaShopWebServiceError:
                pass

    def _check_refunds(self, id_customer, id_order):
        backend_adapter = self.get_connector_unit_for_model(
            GenericAdapter, 'prestashop.refund'
        )
        filters = {'filter[id_customer]': id_customer}
        refund_ids = backend_adapter.search(filters=filters)
        for refund_id in refund_ids:
            refund = backend_adapter.read(refund_id)
            if refund['id_order'] == id_order:
                continue
            self._check_dependency(refund_id, 'prestashop.refund')

    def _has_to_skip(self):
        """ Return True if the import can be skipped """
        if self._get_openerp_id():
            return True
        rules = self.get_connector_unit_for_model(SaleImportRule)
        return rules.check(self.prestashop_record)


@prestashop
class TranslatableRecordImport(PrestashopImportSynchronizer):
    """ Import one translatable record """
    _model_name = []

    _translatable_fields = {}

    _default_language = 'en_US'

    def __init__(self, environment):
        """
        :param environment: current environment (backend, session, ...)
        :type environment: :py:class:`connector.connector.ConnectorEnvironment`
        """
        super(TranslatableRecordImport, self).__init__(environment)
        self.main_lang_data = None
        self.main_lang = None
        self.other_langs_data = None

    def _get_oerp_language(self, prestashop_id):
        language_binder = self.get_binder_for_model('prestashop.res.lang')
        erp_language = language_binder.to_openerp(prestashop_id)
        return erp_language

    def find_each_language(self, record):
        languages = {}
        for field in self._translatable_fields[self.connector_env.model_name]:
            # TODO FIXME in prestapyt
            if not isinstance(record[field]['language'], list):
                record[field]['language'] = [record[field]['language']]
            for language in record[field]['language']:
                if not language or language['attrs']['id'] in languages:
                    continue
                erp_lang = self._get_oerp_language(language['attrs']['id'])
                if erp_lang:
                    languages[language['attrs']['id']] = erp_lang.code
        return languages

    def _split_per_language(self, record, fields=None):
        """Split record values by language.

        @param record: a record from PS
        @param fields: fields whitelist
        @return a dictionary with the following structure:

            'en_US': {
                'field1': value_en,
                'field2': value_en,
            },
            'it_IT': {
                'field1': value_it,
                'field2': value_it,
            }
        """
        split_record = {}
        languages = self.find_each_language(record)
        if not languages:
            raise FailedJobError(
                _('No language mapping defined. '
                  'Run "Synchronize base data".')
            )
        model_name = self.connector_env.model_name
        for language_id, language_code in languages.iteritems():
            split_record[language_code] = record.copy()
        _fields = self._translatable_fields[model_name]
        if fields:
            _fields = [x for x in _fields if x in fields]
        for field in _fields:
            for language in record[field]['language']:
                current_id = language['attrs']['id']
                code = languages.get(current_id)
                if not code:
                    # TODO: be nicer here.
                    # Currently if you have a language in PS
                    # that is not present in odoo
                    # the basic metadata sync is broken.
                    # We should present skip the language
                    # and maybe show a message to users.
                    raise FailedJobError(
                        _('No language could be found for the Prestashop lang '
                          'with id "%s". Run "Synchronize base data" again.') %
                        (current_id,)
                    )
                split_record[code][field] = language['value']
        return split_record

    def _create_context(self):
        context = super(TranslatableRecordImport, self)._create_context()
        if self.main_lang:
            context['lang'] = self.main_lang
        return context

    def _map_data(self):
        """ Returns an instance of
        :py:class:`~openerp.addons.connector.unit.mapper.MapRecord`

        """
        return self.mapper.map_record(self.main_lang_data)

    def _import(self, binding, **kwargs):
        """ Import the external record.

        Can be inherited to modify for instance the session
        (change current user, values in context, ...)

        """
        # split prestashop data for every lang
        split_record = self._split_per_language(self.prestashop_record)
        if self._default_language in split_record:
            self.main_lang_data = split_record[self._default_language]
            self.main_lang = self._default_language
            del split_record[self._default_language]
        else:
            self.main_lang, self.main_lang_data = split_record.popitem()

        self.other_langs_data = split_record

        super(TranslatableRecordImport, self)._import(binding)

    def _after_import(self, binding):
        """ Hook called at the end of the import """
        for lang_code, lang_record in self.other_langs_data.iteritems():
            map_record = self.mapper.map_record(lang_record)
            binding.with_context(
                lang=lang_code,
                connector_no_export=True,
            ).write(map_record.values())


@prestashop
class PartnerCategoryRecordImport(PrestashopImportSynchronizer):
    """ Import one translatable record """
    _model_name = [
        'prestashop.res.partner.category',
    ]

    _translatable_fields = {
        'prestashop.res.partner.category': ['name'],
    }

    def _after_import(self, erp_id):
        record = self._get_prestashop_data()
        if float(record['reduction']):
            import_record(
                self.session,
                'prestashop.groups.pricelist',
                self.backend_record.id,
                record['id']
            )


@prestashop
class ProductCategoryImport(TranslatableRecordImport):
    _model_name = [
        'prestashop.product.category',
    ]

    _translatable_fields = {
        'prestashop.product.category': [
            'name',
            'description',
            'link_rewrite',
            'meta_description',
            'meta_keywords',
            'meta_title'
        ],
    }

    def _import_dependencies(self):
        record = self.prestashop_record
        if record['id_parent'] != '0':
            try:
                self._check_dependency(record['id_parent'],
                                       'prestashop.product.category')
            except PrestaShopWebServiceError:
                pass


@prestashop
class ProductRecordImport(TranslatableRecordImport):
    """ Import one translatable record """
    _model_name = [
        'prestashop.product.product',
    ]

    _translatable_fields = {
        'prestashop.product.product': [
            'name',
            'description',
            'link_rewrite',
            'description_short',
        ],
    }

    def _after_import(self, erp_id):
        self.import_combinations()
        self.import_images()
        self.import_default_image()
        self.import_bundle()
        self.import_supplierinfo(erp_id)

    def import_bundle(self):
        record = self._get_prestashop_data()
        bundle = record.get('associations', {}).get('product_bundle', {})
        if 'products' not in bundle:
            return
        import_record(
            self.session,
            'prestashop.mrp.bom',
            self.backend_record.id,
            record['id']
        )

    def import_combinations(self):
        prestashop_record = self._get_prestashop_data()
        associations = prestashop_record.get('associations', {})

        combinations = associations.get('combinations', {}).get(
            'combinations', [])
        if not isinstance(combinations, list):
            combinations = [combinations]
        for combination in combinations:
            import_record(
                self.session,
                'prestashop.product.combination',
                self.backend_record.id,
                combination['id']
            )

    def import_images(self):
        prestashop_record = self._get_prestashop_data()
        associations = prestashop_record.get('associations', {})
        images = associations.get('images', {}).get('image', {})
        if not isinstance(images, list):
            images = [images]
        for image in images:
            if image.get('id'):
                import_product_image.delay(
                    self.session,
                    'prestashop.product.image',
                    self.backend_record.id,
                    prestashop_record['id'],
                    image['id'],
                    priority=10,
                )

    def import_supplierinfo(self, erp_id):
        ps_id = self._get_prestashop_data()['id']
        filters = {
            'filter[id_product]': ps_id,
            'filter[id_product_attribute]': 0
        }
        import_batch(
            self.session,
            'prestashop.product.supplierinfo',
            self.backend_record.id,
            filters=filters
        )
        product = self.session.browse('prestashop.product.product', erp_id)
        ps_supplierinfo_ids = self.session.search(
            'prestashop.product.supplierinfo',
            [('product_id', '=', product.openerp_id.id)]
        )
        ps_supplierinfos = self.session.browse(
            'prestashop.product.supplierinfo', ps_supplierinfo_ids
        )
        for ps_supplierinfo in ps_supplierinfos:
            try:
                ps_supplierinfo.resync()
            except PrestaShopWebServiceError:
                ps_supplierinfo.openerp_id.unlink()

    def import_default_image(self):
        record = self._get_prestashop_data()
        if record['id_default_image']['value'] == '':
            return
        adapter = self.get_connector_unit_for_model(
            PrestaShopCRUDAdapter,
            'prestashop.product.image'
        )
        binder = self.get_binder_for_model()
        product_id = binder.to_openerp(record['id'])
        try:
            image = adapter.read(record['id'],
                                 record['id_default_image']['value'])
            self.session.write(
                'prestashop.product.product',
                [product_id],
                {"image": image['content']}
            )
        except PrestaShopWebServiceError:
            pass
        except IOError:
            pass

    def _import_dependencies(self):
        self._import_default_category()
        self._import_categories()
        self._import_attribute_set()

    def _import_attribute_set(self):
        record = self.prestashop_record

        combinations = record.get('associations', {}).get(
            'combinations', {}).get('combinations', [])
        if len(combinations) == 0:
            return

        splitted_record = self._split_per_language(self.prestashop_record)
        if self._default_language in splitted_record:
            name = splitted_record[self._default_language]['name']
        else:
            name = splitted_record.values()[0]['name']

        product_model_id = self.get_product_model_id()
        attribute_group = {
            'model_id': product_model_id,
            'name': 'Combinations options',
            'sequence': 0,
            'attribute_ids': [],
        }
        attribute_set = {
            'model_id': product_model_id,
            'name': name + ' Options',
            'attribute_group_ids': [(0, 0,  attribute_group)],
        }
        attribute_set_id = self.session.create('attribute.set', attribute_set)
        self.prestashop_record['attribute_set_id'] = attribute_set_id

    def get_product_model_id(self):
        ids = self.session.search('ir.model', [
            ('model', '=', 'product.product')]
        )
        assert len(ids) == 1
        return ids[0]

    def _import_default_category(self):
        record = self.prestashop_record
        if int(record['id_category_default']):
            try:
                self._check_dependency(record['id_category_default'],
                                       'prestashop.product.category')
            except PrestaShopWebServiceError:
                pass

    def _import_categories(self):
        record = self.prestashop_record
        associations = record.get('associations', {})
        categories = associations.get('categories', {}).get('category', [])
        if not isinstance(categories, list):
            categories = [categories]
        for category in categories:
            self._check_dependency(category['id'],
                                   'prestashop.product.category')


@prestashop
class SaleOrderStateImport(TranslatableRecordImport):
    """ Import one translatable record """
    _model_name = [
        'prestashop.sale.order.state',
    ]

    _translatable_fields = {
        'prestashop.sale.order.state': [
            'name',
        ],
    }


@prestashop
class ProductImageImport(PrestashopImportSynchronizer):
    _model_name = [
        'prestashop.product.image',
    ]

    def _get_prestashop_data(self):
        """ Return the raw Magento data for ``self.prestashop_id`` """
        return self.backend_adapter.read(self.product_id, self.image_id)

    def run(self, product_id, image_id):
        self.product_id = product_id
        self.image_id = image_id

        try:
            super(ProductImageImport, self).run(image_id)
        except PrestaShopWebServiceError:
            pass


@prestashop
class SaleOrderLineRecordImport(PrestashopImportSynchronizer):
    _model_name = [
        'prestashop.sale.order.line',
    ]

    def run(self, prestashop_record, order_id):
        """ Run the synchronization

        :param prestashop_record: record from Prestashop sale order
        """
        self.prestashop_record = prestashop_record

        skip = self._has_to_skip()
        if skip:
            return skip

        # import the missing linked resources
        self._import_dependencies()

        self.mapper.convert(self.prestashop_record)
        record = self.mapper.data
        record['order_id'] = order_id

        # special check on data before import
        self._validate_data(record)

        erp_id = self._create(record)
        self._after_import(erp_id)


@prestashop
class ProductPricelistImport(TranslatableRecordImport):
    _model_name = [
        'prestashop.groups.pricelist',
    ]

    _translatable_fields = {
        'prestashop.groups.pricelist': ['name'],
    }

    def _run_record(self, prestashop_record, lang_code, erp_id=None):
        return super(ProductPricelistImport, self)._run_record(
            prestashop_record, lang_code, erp_id=erp_id
        )


@job(default_channel='root.prestashop')
def import_batch(session, model_name, backend_id, filters=None, **kwargs):
    """ Prepare a batch import of records from PrestaShop """
    env = get_environment(session, model_name, backend_id)
    #backend = session.pool.get('prestashop.backend').browse(cr, uid, backend_id)
    #env = backend.get_environment(model_name, session=session)
    importer = env.get_connector_unit(BatchImportSynchronizer)
    return importer.run(filters=filters, **kwargs)


@job(default_channel='root.prestashop')
def import_record(
        session, model_name, backend_id, prestashop_id, **kwargs):
    """ Import a record from Prestashop """
    env = get_environment(session, model_name, backend_id)
    importer = env.get_connector_unit(PrestashopImportSynchronizer)
    return importer.run(prestashop_id, **kwargs)

@job
def import_product_image(session, model_name, backend_id, product_id,
                         image_id):
    """Import a product image"""
    env = get_environment(session, model_name, backend_id)
    importer = env.get_connector_unit(PrestashopImportSynchronizer)
    importer.run(product_id, image_id)


@job
def import_customers_since(session, backend_id, since_date=None):
    """ Prepare the import of partners modified on Prestashop """

    filters = None
    if since_date:
        filters = {'date': '1', 'filter[date_upd]': '>[%s]' % (since_date)}
    now_fmt = datetime.now().strftime(DEFAULT_SERVER_DATETIME_FORMAT)
    import_batch(
        session, 'prestashop.res.partner.category', backend_id, filters
    )
    import_batch(
        session, 'prestashop.res.partner', backend_id, filters, priority=15
    )

    session.pool.get('prestashop.backend').write(
        session.cr,
        session.uid,
        backend_id,
        {'import_partners_since': now_fmt},
        context=session.context
    )


@job
def import_orders_since(session, backend_id, since_date=None):
    """ Prepare the import of orders modified on Prestashop """

    filters = None
    if since_date:
        filters = {'date': '1', 'filter[date_upd]': '>[%s]' % (since_date)}
    import_batch(
        session,
        'prestashop.sale.order',
        backend_id,
        filters,
        priority=10,
        max_retries=0
    )

    if since_date:
        filters = {'date': '1', 'filter[date_add]': '>[%s]' % (since_date)}
    try:
        import_batch(session, 'prestashop.mail.message', backend_id, filters)
    except:
        pass

    now_fmt = datetime.now().strftime(DEFAULT_SERVER_DATETIME_FORMAT)
    session.pool.get('prestashop.backend').write(
        session.cr,
        session.uid,
        backend_id,
        {'import_orders_since': now_fmt},
        context=session.context
    )


@job
def import_products(session, backend_id, since_date):
    filters = None
    if since_date:
        filters = {'date': '1', 'filter[date_upd]': '>[%s]' % (since_date)}
    now_fmt = datetime.now().strftime(DEFAULT_SERVER_DATETIME_FORMAT)
    import_batch(
        session,
        'prestashop.product.category',
        backend_id,
        filters,
        priority=15
    )
    import_batch(
        session,
        'prestashop.product.product',
        backend_id,
        filters,
        priority=15
    )
    session.pool.get('prestashop.backend').write(
        session.cr,
        session.uid,
        backend_id,
        {'import_products_since': now_fmt},
        context=session.context
    )


@job
def import_refunds(session, backend_id, since_date):
    filters = None
    if since_date:
        filters = {'date': '1', 'filter[date_upd]': '>[%s]' % (since_date)}
    now_fmt = datetime.now().strftime(DEFAULT_SERVER_DATETIME_FORMAT)
    import_batch(session, 'prestashop.refund', backend_id, filters)
    session.pool.get('prestashop.backend').write(
        session.cr,
        session.uid,
        backend_id,
        {'import_refunds_since': now_fmt},
        context=session.context
    )


@job
def import_suppliers(session, backend_id, since_date):
    filters = None
    if since_date:
        filters = {'date': '1', 'filter[date_upd]': '>[%s]' % (since_date)}
    now_fmt = datetime.now().strftime(DEFAULT_SERVER_DATETIME_FORMAT)
    import_batch(session, 'prestashop.supplier', backend_id, filters)
    import_batch(session, 'prestashop.product.supplierinfo', backend_id)
    session.pool.get('prestashop.backend').write(
        session.cr,
        session.uid,
        backend_id,
        {'import_suppliers_since': now_fmt},
        context=session.context
    )


@job
def import_carriers(session, backend_id):
    import_batch(
        session, 'prestashop.delivery.carrier', backend_id, priority=5
    )


@job
def export_product_quantities(session, ids):
    for model in ['product', 'combination']:
        model_obj = session.pool['prestashop.product.' + model]
        model_ids = model_obj.search(
            session.cr,
            session.uid,
            [('backend_id', 'in', ids)],
            context=session.context
        )
        model_obj.recompute_prestashop_qty(
            session.cr, session.uid, model_ids, context=session.context
        )
