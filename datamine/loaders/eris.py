from . import Loader

import pandas as pd
import numpy as np

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
               'EffectiveYearMonth', 'UnpaidFixedAccrualStartDate', 'UnpaidFixedAccrual', 'UnpaidFloatingAccrualStartDate', 'UnpaidFloatingAccrual', 'NetUnpaidFixedFloatingAccrual', 'NPV(A)lessNetUnpaidFixedFloatingAccrual', 'AccruedCoupons(B)plusNetUnpaidFixedFloatingAccrual']

    dtypes = {'category': ('Symbol', 'ResetRateDescriptor', 'ExchangeSymbol (EX005)', 'BloombergTicker', 'EffectiveYearMonth'),
              'int64': ('AccrualDays', 'EffectiveYearMonth', 'Nominal'),
              'float': ('FinalSettlementPrice', 'NPV (A)', 'FixedNPV', 'FloatingNPV', 'Coupon (%)',
                        'FairCoupon (%)', 'FixedPayment', 'FloatingPayment', 'NextFixedPaymentAmount',
                        'PreviousFixingRate', 'NextFloatingPaymentAmount', 'PreviousSettlementPrice',
                        'PreviousErisPAI', 'FedFundsRate (%)', 'DailyIncrementalErisPAI', 'AccruedCoupons (B)',
                        'ErisPAI (C)', 'SettlementPrice (100+A+B-C)', 'InterpolationFactor',
                        'HighTradePrice', 'PV01', 'DV01',
                        'UnpaidFixedAccrual','UnpaidFloatingAccrual','NetUnpaidFixedFloatingAccrual',
                        'NPV(A)lessNetUnpaidFixedFloatingAccrual', 'AccruedCoupons(B)plusNetUnpaidFixedFloatingAccrual'),
              'date:%m/%d/%Y': ('EvaluationDate', 'FirstTradeDate', 'ErisPAIDate',
                                'EffectiveDate', 'CashFlowAlignmentDate', 'MaturityDate',
                                'NextFixedPaymentDate', 'PreviousFixingDate', 'NextFloatingPaymentDate',
                                'NextFixingDate', 'PreviousSettlementDate',
                                'FedFundsDate', 'LastTradeDate', 'FirstFixingDate',
                                'UnpaidFixedAccrualStartDate', 'UnpaidFloatingAccrualStartDate')}

    def _load(self, file):
        df = pd.read_csv(file, low_memory=False)
        if len(df.columns) == 58:
            col_adjustment = {'UnpaidFixedAccrualStartDate' : np.datetime64(), 'UnpaidFixedAccrual' : float(), 'UnpaidFloatingAccrualStartDate' : np.datetime64(), 'UnpaidFloatingAccrual' : float(), 'NetUnpaidFixedFloatingAccrual' : float(), 'NPV(A)lessNetUnpaidFixedFloatingAccrual' : float(), 'AccruedCoupons(B)plusNetUnpaidFixedFloatingAccrual' : float()}
            for k, v in col_adjustment.items():
                df.insert(len(df.columns), k, v)
        return df

erisLoader = ErisLoader()
