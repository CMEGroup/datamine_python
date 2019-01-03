from . import Loader


class ErisLoader(Loader):
    dataset = 'ERIS'
    fileglob = 'ERIS_*.csv'

    columns = ['Symbol', 'FinalSettlementPrice', 'EvaluationDate', 'FirstTradeDate',
               'ErisPAIDate', 'EffectiveDate', 'CashFlowAlignmentDate', 'MaturityDate', 'NPV (A)',
               'FixedNPV', 'FloatingNPV', 'Coupon (%)', 'FairCoupon (%)', 'FixedPayment', 'FloatingPayment',
               'NextFixedPaymentDate', 'NextFixedPaymentAmount', 'PreviousFixingDate', 'PreviousFixingRate',
               'NextFloatingPaymentDate', 'NextFloatingPaymentAmount', 'NextFixingDate', 'PreviousSettlementDate',
               'PreviousSettlementPrice', 'PreviousErisPAI', 'FedFundsDate', 'FedFundsRate (%)', 'AccrualDays',
               'DailyIncrementalErisPAI', 'AccruedCoupons (B)', 'ErisPAI (C)', 'SettlementPrice (100+A+B-C)',
               'RFQ NPV TickSize ($)', 'Nominal', 'ResetRateDescriptor', 'InterpolationFactor', 'HighTradePrice',
               'LowTradePrice', 'LastTradePrice', 'DailyContractVolume', 'Tag55(T)', 'Tag65(T)', 'Tag55(T+1)',
               'Tag65(T+1)', 'LastTradeDate', 'InitialSpeculatorMargin', 'SecondarySpeculatorMargin',
               'InitialHedgerMargin', 'SecondaryHedgerMargin', 'ExchangeSymbol (EX005)', 'BloombergTicker',
               'FirstFixingDate', 'Category', 'BenchmarkContractName', 'PV01', 'DV01', 'ShortName',
               'EffectiveYearMonth']

    dtypes = {'category': ('Symbol', 'ResetRateDescriptor', 'ExchangeSymbol (EX005)', 'BloombergTicker'),
              'int64': ('AccrualDays', 'EffectiveYearMonth', 'Nominal'),
              'float': ('FinalSettlementPrice', 'NPV (A)', 'FixedNPV', 'FloatingNPV', 'Coupon (%)',
                        'FairCoupon (%)', 'FixedPayment', 'FloatingPayment', 'NextFixedPaymentAmount',
                        'PreviousFixingRate', 'NextFloatingPaymentAmount', 'PreviousSettlementPrice',
                        'PreviousErisPAI', 'FedFundsRate (%)', 'DailyIncrementalErisPAI', 'AccruedCoupons (B)',
                        'ErisPAI (C)', 'SettlementPrice (100+A+B-C)', 'InterpolationFactor',
                        'HighTradePrice', 'PV01', 'DV01'),
              'date:%m/%d/%Y': ('EvaluationDate', 'FirstTradeDate', 'ErisPAIDate',
                                'EffectiveDate', 'CashFlowAlignmentDate', 'MaturityDate',
                                'NextFixedPaymentDate', 'PreviousFixingDate', 'NextFloatingPaymentDate',
                                'NextFixingDate', 'PreviousSettlementDate',
                                'FedFundsDate', 'LastTradeDate', 'FirstFixingDate')}


erisLoader = ErisLoader()
