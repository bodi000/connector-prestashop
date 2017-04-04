# -*- coding: utf-8 -*-
# License AGPL-3.0 or later (http://www.gnu.org/licenses/agpl).

import logging
import pytz
from datetime import datetime

from openerp.osv import fields, orm

from openerp.tools import DEFAULT_SERVER_DATETIME_FORMAT
from openerp.addons.connector.session import ConnectorSession
from openerp.addons.connector.connector import Environment
from ...unit.import_synchronizer import (
    import_batch,
    import_record,
    import_customers_since,
    import_orders_since,
    import_products,
    import_carriers,
    import_suppliers,
    import_refunds,
    export_product_quantities,

    )
from ...product import import_inventory

from ...unit.direct_binder import DirectBinder
from ...connector import get_environment

from ...backend import prestashop
from ...unit.backend_adapter import GenericAdapter

_logger = logging.getLogger(__name__)


class prestashop_backend(orm.Model):
    _name = 'prestashop.backend'
    _doc = 'Prestashop Backend'
    _inherit = 'connector.backend'

    _backend_type = 'prestashop'

    def _select_versions(self, cr, uid, context=None):
        """ Available versions

        Can be inherited to add custom versions.
        """
        return [
            ('1.5', '< 1.6.0.9'),
            ('1.6.0.9', '1.6.0.9 - 1.6.0.10'),
            ('1.6.0.11', '>= 1.6.0.11 - <1.6.1.2'),
            ('1.6.1.2', '=1.6.1.2')
        ]

    _columns = {
        'version': fields.selection(
            _select_versions,
            string='Version',
            required=True),
        'location': fields.char('Location'),
        'webservice_key': fields.char(
            'Webservice key',
            help="You have to put it in 'username' of the PrestaShop "
                 "Webservice api path invite"
        ),
        'warehouse_id': fields.many2one(
            'stock.warehouse',
            'Warehouse',
            required=True,
            help='Warehouse used to compute the stock quantities.'
        ),
        'taxes_included': fields.boolean("Use tax included prices"),
        'import_partners_since': fields.datetime('Import partners since'),
        'import_orders_since': fields.datetime('Import Orders since'),
        'import_products_since': fields.datetime('Import Products since'),
        'import_refunds_since': fields.datetime('Import Refunds since'),
        'import_suppliers_since': fields.datetime('Import Suppliers since'),
        'language_ids': fields.one2many(
            'prestashop.res.lang',
            'backend_id',
            'Languages'
        ),
        'company_id': fields.many2one('res.company', 'Company', select=1, required=True),
        'discount_product_id': fields.many2one('product.product', 'Dicount Product', select=1, required=False),
        'shipping_product_id': fields.many2one('product.product', 'Shipping Product', select=1, required=False),
    }

    _defaults = {
        'company_id': lambda s,cr,uid,c: s.pool.get('res.company')._company_default_get(cr, uid, 'prestashop.backend', context=c),
    }

    def get_environment(self, cr, uid, model_name, session=None, context=None):
        self.ensure_one()
        if not session:
            session = ConnectorSession(cr, uid, context=context)
        return Environment(self, cr, uid, session, model_name)

    def synchronize_metadata(self, cr, uid, ids, context=None):
        if not hasattr(ids, '__iter__'):
            ids = [ids]
        session = ConnectorSession(cr, uid, context=context)
        for backend_id in ids:
            for model in ('prestashop.shop.group',
                          'prestashop.shop'):
                # import directly, do not delay because this
                # is a fast operation, a direct return is fine
                # and it is simpler to import them sequentially
                import_batch(session, model, backend_id)
        return True

    def synchronize_basedata(self, cr, uid, ids, context=None):
        if not hasattr(ids, '__iter__'):
            ids = [ids]
        session = ConnectorSession(cr, uid, context=context)
        for backend_id in ids:
            for model_name in [
                'prestashop.res.lang',
                'prestashop.res.country',
                'prestashop.res.currency',
                'prestashop.account.tax',
            ]:
                env = get_environment(session, model_name, backend_id)
                directBinder = env.get_connector_unit(DirectBinder)
                directBinder.run()

            import_batch(session, 'prestashop.account.tax.group', backend_id)
            import_batch(session, 'prestashop.sale.order.state', backend_id)
        return True

    def _date_as_user_tz(self, cr, uid, dtstr):
        if not dtstr:
            return None
        users_obj = self.pool.get('res.users')
        user = users_obj.browse(cr, uid, uid)
        timezone = pytz.timezone(user.partner_id.tz or 'utc')
        dt = datetime.strptime(dtstr, DEFAULT_SERVER_DATETIME_FORMAT)
        dt = pytz.utc.localize(dt)
        dt = dt.astimezone(timezone)
        return dt

    def import_customers_since(self, cr, uid, ids, context=None):
        if not hasattr(ids, '__iter__'):
            ids = [ids]
        session = ConnectorSession(cr, uid, context=context)
        for backend_record in self.browse(cr, uid, ids, context=context):
            since_date = backend_record.import_partners_since
            import_customers_since.delay(
                session,
                backend_record.id,
                since_date,
                priority=10,
            )

        return True

    def import_products(self, cr, uid, ids, context=None):
        if not hasattr(ids, '__iter__'):
            ids = [ids]
        session = ConnectorSession(cr, uid, context=context)
        for backend_record in self.browse(cr, uid, ids, context=context):
            since_date = backend_record.import_products_since
            import_products.delay(session, backend_record.id, since_date, priority=10)
        return True

    def import_carriers(self, cr, uid, ids, context=None):
        if not hasattr(ids, '__iter__'):
            ids = [ids]
        session = ConnectorSession(cr, uid, context=context)
        for backend_id in ids:
            import_carriers.delay(session, backend_id, priority=10)
        return True

    def update_product_stock_qty(self, cr, uid, ids, context=None):
        if not hasattr(ids, '__iter__'):
            ids = [ids]
        session = ConnectorSession(cr, uid, context=context)
        export_product_quantities.delay(session, ids)
        return True

    def import_stock_qty(self, cr, uid, ids, context=None):
        if not hasattr(ids, '__iter__'):
            ids = [ids]
        session = ConnectorSession(cr, uid, context=context)
        for backend_id in ids:
            import_inventory.delay(session, backend_id)

    def import_sale_orders(self, cr, uid, ids, context=None):
        if not hasattr(ids, '__iter__'):
            ids = [ids]
        session = ConnectorSession(cr, uid, context=context)
        for backend_record in self.browse(cr, uid, ids, context=context):
            since_date = backend_record.import_orders_since
            import_orders_since.delay(
                session,
                backend_record.id,
                since_date,
                priority=5,
            )
        return True

    def import_payment_methods(self, cr, uid, ids, context=None):
        if not hasattr(ids, '__iter__'):
            ids = [ids]
        session = ConnectorSession(cr, uid, context=context)
        for backend_record in self.browse(cr, uid, ids, context=context):
            import_batch.delay(session, 'payment.method', backend_record.id)
        return True

    def import_refunds(self, cr, uid, ids, context=None):
        if not hasattr(ids, '__iter__'):
            ids = [ids]
        session = ConnectorSession(cr, uid, context=context)
        for backend_record in self.browse(cr, uid, ids, context=context):
            since_date = backend_record.import_refunds_since
            import_refunds.delay(session, backend_record.id, since_date)
        return True

    def import_suppliers(self, cr, uid, ids, context=None):
        if not hasattr(ids, '__iter__'):
            ids = [ids]
        session = ConnectorSession(cr, uid, context=context)
        for backend_record in self.browse(cr, uid, ids, context=context):
            since_date = backend_record.import_suppliers_since
            import_suppliers.delay(session, backend_record.id, since_date)
        return True

    def _scheduler_launch(self, cr, uid, callback, domain=None,
                          context=None):
        if domain is None:
            domain = []
        ids = self.search(cr, uid, domain, context=context)
        if ids:
            callback(cr, uid, ids, context=context)

    def _scheduler_update_product_stock_qty(self, cr, uid, domain=None,
                                            context=None):
        self._scheduler_launch(cr, uid, self.update_product_stock_qty,
                               domain=domain, context=context)

    def _scheduler_import_sale_orders(self, cr, uid, domain=None, context=None):
        self._scheduler_launch(cr, uid, self.import_sale_orders, domain=domain,
                               context=context)

    def _scheduler_import_customers(self, cr, uid, domain=None, context=None):
        self._scheduler_launch(cr, uid, self.import_customers_since,
                               domain=domain, context=context)

    def _scheduler_import_products(self, cr, uid, domain=None, context=None):
        self._scheduler_launch(cr, uid, self.import_products, domain=domain,
                               context=context)

    def _scheduler_import_carriers(self, cr, uid, domain=None, context=None):
        self._scheduler_launch(cr, uid, self.import_carriers, domain=domain,
                               context=context)

    def _scheduler_import_payment_methods(self, cr, uid, domain=None, context=None):
        self._scheduler_launch(cr, uid, self.import_payment_methods,
                               domain=domain, context=context)

    def _scheduler_import_refunds(self, cr, uid, domain=None, context=None):
        self._scheduler_launch(cr, uid, self.import_refunds,
                               domain=domain, context=context)

    def _scheduler_import_suppliers(self, cr, uid, domain=None, context=None):
        self._scheduler_launch(cr, uid, self.import_suppliers,
                               domain=domain, context=context)

    def import_record(self, cr, uid, backend_id, model_name, ext_id,
                      context=None):
        session = ConnectorSession(cr, uid, context=context)
        import_record(session, model_name, backend_id, ext_id)
        return True

@prestashop
class NoModelAdapter(GenericAdapter):
    """ Used to test the connection """
    _model_name = 'prestashop.backend'
    _prestashop_model = ''

@prestashop
class ShopGroupAdapter(GenericAdapter):
    _model_name = 'prestashop.shop.group'
    _prestashop_model = 'shop_groups'

