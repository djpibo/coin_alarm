class AlarmService:
    def percentage_alarm(self, key: str):
        keys = r.keys(key)
        for item in keys:
            dynamic_market = r.get(item).decode("UTF-8")
            # 쿼리 작성
            query = {
                "market": dynamic_market
            }
            # 가장 최근 trade_time을 찾기 위해 정렬
            sort_order = [("timestamp", -1)]
            result_prev = collection.find(query, sort=sort_order).skip(1).limit(1)[0]  # return Cursor to dict (first)
            result_curr = collection.find_one(query, sort=sort_order)  # <class 'dict'>
            print(result_prev.get("change_rate"))
            # explain_result = result_prev.explain()
            # print(explain_result)  # This will provide details about how the query is executed, including index usage
            gap = result_curr.get("change_rate") - result_prev.get("change_rate")
            if gap > 0.01:
                if result_curr.get("change") == "RISE":
                    save_gap_list(result_curr, gap, 1)
                    coin_up_list.append(dynamic_market)
                else:
                    save_gap_list(result_curr, gap, 0)
                    coin_down_list.append(dynamic_market)

        if coin_up_list:
            print(coin_up_list)
            toaster.show_toast(f"상승폭 1% 이상 알림", f"대상 코인 : {coin_up_list}", duration=10)
        coin_up_list.clear()

        if coin_down_list:
            print(coin_down_list)
            toaster.show_toast(f"하락폭 1% 이상 알림", f"대상 코인 : {coin_down_list}", duration=10)
        coin_down_list.clear()