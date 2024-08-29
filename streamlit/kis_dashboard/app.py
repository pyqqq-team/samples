from pyqqq.brokerage.kis.oauth import KISAuth
from pyqqq.brokerage.kis.simple import KISSimpleDomesticStock
from typing import Optional
import os
import dotenv
import datetime as dtm
import pandas as pd
import pyqqq
import streamlit as st
import yaml
from yaml.loader import SafeLoader

def get_balance(include_sold=False):
    ctx_area_fk100 = ""
    ctx_area_nk100 = ""
    tr_cont = ""
    fetching = True

    positions = []

    while fetching:

        r = stock_api.inquire_balance(
            cano,
            acnt_prdt_cd,
            "02",
            ctx_area_fk100=ctx_area_fk100,
            ctx_area_nk100=ctx_area_nk100,
            tr_cont=tr_cont,
        )

        for data in r["output1"]:
            holding_qty = int(data["hldg_qty"])
            if not include_sold and holding_qty == 0:
                continue

            positions.append(
                {
                    "종목코드": data["pdno"],
                    "종목명": data["prdt_name"],
                    "평가손익": data["evlu_pfls_amt"],
                    "수익률": data["evlu_pfls_rt"],
                    "보유수량": holding_qty,
                    "평가금액": data["evlu_amt"],
                    "현재가": data["prpr"],
                    "매입단가": int(data["pchs_avg_pric"]),
                    "매입금액": data["pchs_amt"],
                    "전일대비": data["bfdy_cprs_icdc"],
                }
            )

        if r["tr_cont"] in ["F", "M"]:
            ctx_area_nk100 = r["ctx_area_nk100"]
            ctx_area_fk100 = r["ctx_area_fk100"]
            tr_cont = "N"
        else:
            fetching = False

    positions_df = pd.DataFrame(positions)
    if len(positions) > 0:
        positions_df = positions_df.sort_values(by="수익률", ascending=False)
        positions_df.set_index("종목코드", inplace=True)

    output2 = r["output2"][0]

    purchase_amount = output2["pchs_amt_smtl_amt"]
    current_amount = output2["evlu_amt_smtl_amt"]
    pnl_amount = output2["evlu_pfls_smtl_amt"]
    pnl_rate = (
        int(pnl_amount) / int(purchase_amount) * 100 if purchase_amount > 0 else 0
    )

    statistics = {
        "매입금액": purchase_amount,
        "평가금액": current_amount,
        "평가손익": pnl_amount,
        "수익률": pnl_rate,
    }

    net_df = pd.DataFrame([statistics])
    net_df.rename(index={0: "총합"}, inplace=True)

    return positions_df, net_df


def get_today_pnl_and_trades(target_date: dtm.date = None):
    ctx_area_fk100 = ""
    ctx_area_nk100 = ""
    tr_cont = ""

    if target_date is None:
        target_date = dtm.date.today()

    trades = []

    fetching = True

    asset_codes = set()

    while fetching:
        r = stock_api.inquire_period_trade_profit(
            cano,
            acnt_prdt_cd,
            target_date,
            target_date,
            ctx_area_fk100=ctx_area_fk100,
            ctx_area_nk100=ctx_area_nk100,
            tr_cont=tr_cont,
        )

        for data in r["output1"]:
            if data["sll_qty"] == 0:
                continue

            row = {
                "종목코드": data["pdno"][-6:],
                "종목명": data["prdt_name"],
                "실현손익": data["rlzt_pfls"],
                "손익률": data["pfls_rt"],
                "매수금액": data["buy_amt"],
                "매도금액": data["sll_amt"],
                "매수수량": data["buy_qty"],
                "매도수량": data["sll_qty"],
                "매수단가": int(
                    (data["buy_amt"] / data["buy_qty"]) if data["buy_qty"] > 0 else 0
                ),
                "매도단가": int(
                    (data["sll_amt"] / data["sll_qty"]) if data["sll_qty"] > 0 else 0
                ),
                "현재가": 0,
                "수수료": data["fee"],
                "제세금": data["tl_tax"],
                "매매일": data["trad_dt"],
            }
            trades.append(row)
            asset_codes.add(row["종목코드"][-6:])

        if r["tr_cont"] in ["F", "M"]:
            ctx_area_nk100 = r["ctx_area_nk100"]
            ctx_area_fk100 = r["ctx_area_fk100"]
            tr_cont = "N"
        else:
            fetching = False

    if asset_codes:
        current_prices_df = simple_api.get_price_for_multiple_stock(list(asset_codes))

    if trades:
        trades_df = pd.DataFrame(trades)
        trades_df.sort_values(by="실현손익", ascending=False, inplace=True)
        trades_df.set_index("종목코드", inplace=True)

        for code in asset_codes:
            trades_df.loc[code, "현재가"] = current_prices_df.loc[code, "current_price"]
    else:
        trades_df = pd.DataFrame()

    output2 = r["output2"]
    net_data = {
        "매도금액": output2["sll_excc_amt_smtl"],
        "매수금액": output2["buy_excc_amt_smtl"],
        "매매비용": output2["tot_fee"],
        "제세금": output2["tot_tltx"],
        "실현손익": output2["tot_rlzt_pfls"],
        "실현손익률": output2["tot_pftrt"],
    }
    net_df = pd.DataFrame([net_data])
    net_df.rename(index={0: "총합"}, inplace=True)

    return trades_df, net_df


def get_period_profit(start_date: dtm.date, end_date: dtm.date):
    fetching = True
    tr_cont = ""
    ctx_area_fk100 = ""
    ctx_area_nk100 = ""

    bucket = []

    while fetching:
        r = stock_api.inquire_period_profit(
            cano,
            acnt_prdt_cd,
            start_date,
            end_date,
            tr_cont=tr_cont,
            ctx_area_fk100=ctx_area_fk100,
            ctx_area_nk100=ctx_area_nk100,
        )

        for data in r["output1"]:
            bucket.append(data)

        if r["tr_cont"] in ["F", "M"]:
            ctx_area_nk100 = r["ctx_area_nk100"]
            ctx_area_fk100 = r["ctx_area_fk100"]
            tr_cont = "N"
        else:
            fetching = False

    df = pd.DataFrame(bucket)
    df.sort_values(by="trad_dt", inplace=True)

    # rlzt_pfls 컬럼 값을 누적하는 새로운 컬럼을 만들어줍니다.
    df["누적실현손익"] = df["rlzt_pfls"].cumsum()
    df.rename(
        columns={
            "trad_dt": "매매일자",
            "buy_amt": "매수금액",
            "sll_amt": "매도금액",
            "rlzt_pfls": "실현손익",
            "fee": "수수료",
            "tl_tax": "제세금",
            "pfls_rt": "수익률",
            "sll_qty1": "매도수량",
            "buy_qty1": "매수수량",
        },
        inplace=True,
    )

    df.set_index("매매일자", inplace=True)

    return df


def get_order_history(target_date: Optional[dtm.date] = None):
    ctx_area_fk100 = ""
    ctx_area_nk100 = ""
    tr_cont = ""

    if target_date is None:
        target_date = dtm.date.today()

    orders = []
    fetching = True

    while fetching:
        r = stock_api.inquire_daily_ccld(cano, acnt_prdt_cd, target_date, target_date, ctx_area_fk100=ctx_area_fk100, ctx_area_nk100=ctx_area_nk100, tr_cont=tr_cont)

        for data in r["output1"]:
            row = {
                "주문일": data["ord_dt"],
                "주문시간": data["ord_tmd"],
                "종목코드": data["pdno"][-6:],
                "종목명": data["prdt_name"],
                "주문량": data["ord_qty"],
                "체결량": data["tot_ccld_qty"],
                "주문단가": data["ord_unpr"],
                "체결평균": data["avg_prvs"],
                "총체결금액": data["tot_ccld_amt"],
                "구분": data["sll_buy_dvsn_cd_name"],
                "주문유형": data["ord_dvsn_name"],
                "미체결량": data["rmn_qty"],
                "취소수량": data["cncl_cfrm_qty"],
                "주문번호": data["odno"],
                "원주문": data.get("orgn_odno", ""),
            }
            orders.append(row)

        if r["tr_cont"] in ["F", "M"]:
            ctx_area_nk100 = r["ctx_area_nk100"]
            ctx_area_fk100 = r["ctx_area_fk100"]
            tr_cont = "N"
        else:
            fetching = False

    orders_df = pd.DataFrame(orders)

    if orders:
        orders_df.sort_values(by="주문시간", ascending=False, inplace=True)

    return orders_df


def render_position():
    def __render():
        pos_df, pos_net_df = get_balance()

        with placeholder.container():
            st.write(pos_net_df)
            st.write(pos_df)

    st.write(
        """
        ### 보유 주식 잔고
        """
    )
    placeholder = st.empty()
    __render()

def render_today_profit_and_loss():
    def __render():
        trades_df, net_df = get_today_pnl_and_trades()

        with placeholder.container():
            st.write(net_df)
            st.write(trades_df)

    st.write(
        """
        ### 매매손익
        """
    )
    placeholder = st.empty()
    __render()

def render_order_history():
    def __render():
        order_history_df = get_order_history()

        with placeholder.container():
            st.dataframe(order_history_df)

    st.write(
        """
        ### 주문내역
        """
    )
    placeholder = st.empty()
    __render()

def render_period_profit():
    def __render():
        end_date = dtm.date.today()
        # start_date = end_date - dtm.timedelta(days=30)
        start_date = dtm.date(2024, 8, 1)

        df = get_period_profit(start_date, end_date)

        with placeholder.container():
            data_df = df.iloc[::-1]
            data_df = data_df[["매수금액", "매도금액", "실현손익", "수익률", "수수료", "제세금", "누적실현손익"]]
            st.dataframe(data_df)
            st.line_chart(df[["누적실현손익"]])

    st.write(
        """
        ### 기간 손익 일별 합산 조회
        """
    )
    placeholder = st.empty()
    __render()


if __name__ == '__main__':
    dotenv.load_dotenv()

    app_key = os.getenv('KIS_APP_KEY')
    app_secret = os.getenv('KIS_APP_SECRET')
    cano = os.getenv("KIS_CANO")
    acnt_prdt_cd = os.getenv('KIS_ACNT_PRDT_CD')
    pyqqq_api_key = os.getenv('PYQQQ_API_KEY')

    pyqqq.set_api_key(pyqqq_api_key)

    auth = KISAuth(app_key, app_secret)
    simple_api = KISSimpleDomesticStock(auth, cano, acnt_prdt_cd)
    stock_api = simple_api.stock_api

    render_position()
    render_today_profit_and_loss()
    render_order_history()
    render_period_profit()
