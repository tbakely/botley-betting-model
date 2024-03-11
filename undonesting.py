import pandas as pd


class UndoNest:
    @staticmethod
    def _find_nested_keys(data: dict):
        for key, value in data.items():
            if isinstance(value, list):
                yield key
                yield from UndoNest._find_nested_keys(value[0])

    @staticmethod
    def _find_column_keys(data: dict, nested_keys: list[str]):
        if not nested_keys:
            return
        nested_key = nested_keys.pop(0)
        yield [key for key in data.keys() if not key == nested_key]
        yield from UndoNest._find_column_keys(data[nested_key][0], nested_keys)

    @staticmethod
    def _setup_normalized_tables(data, nested_cols, cols):
        result = []
        if not nested_cols:
            return [json_normalize(data)]
        for i in range(len(nested_cols)):
            if nested_cols[i] == nested_cols[0]:
                step = pd.json_normalize(data, nested_cols[i], cols[i], record_prefix=f'{nested_cols[i]}_')
                result.append(step)
            else:
                step = pd.json_normalize(result[i-1].to_dict(orient='records'),
                                        f"{nested_cols[i-1]}_{nested_cols[i]}", 
                                        [cols[0][0]] + [f"{nested_cols[i-1]}_{col}" for col in cols[i]], 
                                        record_prefix=f'{nested_cols[i]}_')
                result.append(step)

        for df in result:
            to_drop = [col for col in df.columns if isinstance(df[col][0], list)]
            df.drop(columns=to_drop, inplace=True)
        
        return result

    @staticmethod
    def _backwards_merge(dataframes: list[pd.DataFrame]):
        if len(dataframes) < 2:
            return dataframes[0]
        common_cols = list(set(dataframes[-1].columns) & set(dataframes[-2].columns))
        dataframes[-2] = pd.merge(dataframes[-1], dataframes[-2], how='outer', on=common_cols)
        next_in_line = dataframes[:-1]
        return UndoNest._backwards_merge(next_in_line)
    
    def __init__(self, json_response: list[dict]):
        if not json_response or not isinstance(json_response, list) or not all(isinstance(d, dict) for d in json_response):
            raise ValueError("Invalid input. Expecting a non-empty list of dictionaries.")
        self.json_response = json_response
        self.nested_keys = list(UndoNest._find_nested_keys(json_response[0]))
        self.column_keys = list(UndoNest._find_column_keys(json_response[0], self.nested_keys.copy()))
        self._normalized_tables = UndoNest._setup_normalized_tables(self.json_response, 
                                                                   self.nested_keys, 
                                                                   self.column_keys)
        self._final_merged_tables = UndoNest._backwards_merge(self._normalized_tables)

    def get_finalized_table(self):
        return self._final_merged_tables