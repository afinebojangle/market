import tensorflow as tf

tf.logging.set_verbosity(tf.logging.INFO)

#whoops, pct_implied_volitility spelled wrong. Fixed in query but when new dataset is made will have to update spellings below.
_CSV_COLUMNS = ['ticker', 'alpha', 'beta', 'trade_type', 'years_to_experiation', 'pct_capm_error',
    'pct_ma_50_volatility', 'stock_price', 'pct_strike_variance', 'option_price', 'pct_implied_volitility',
    'delta', 'gamma', 'theta', 'vega', 'rho', 'phi', 'label']

LABEL_VOCAB = ['-51', '-52', '-53', '-54', '-55', '-56', '-57',
               '-41', '-42', '-43', '-44', '-45', '-46', '-47',
               '-31', '-32', '-33', '-34', '-35', '-36', '-37',
               '-21', '-22', '-23', '-24', '-25', '-26', '-27',
               '-11', '-12', '-13', '-14', '-15', '-16', '-17',
               '11', '12', '13', '14', '15', '16', '17',
               '21', '22', '23', '24', '25', '26', '27',
               '31', '32', '33', '34', '35', '36', '37',
               '41', '42', '43', '44', '45', '46', '47',
               '51', '52', '53', '54', '55', '56', '57']

_LABEL_DEFAULT_TENSOR = tf.constant(0, dtype=tf.float32, shape=(len(LABEL_VOCAB),))

_CSV_COLUMN_DEFAULTS = [[''], [0.0], [0.0], [''], [0.0], [0.0],
                        [0.0], [0.0], [0.0], [0.0], [0.0],
                        [0.0], [0.0], [0.0], [0.0], [0.0], [0.0], [0]]

def build_feature_columns():
    ticker = tf.feature_column.categorical_column_with_hash_bucket('ticker', hash_bucket_size=10000)
    alpha = tf.feature_column.numeric_column('alpha', dtype=tf.float32)
    beta = tf.feature_column.numeric_column('beta', dtype=tf.float32)
    trade_type = tf.feature_column.categorical_column_with_vocabulary_list('trade_type', ['Long Call', 'Short Call', 'Long Put', 'Short Put'])
    years_to_experiation = tf.feature_column.numeric_column('years_to_experiation', dtype=tf.float32)
    pct_capm_error = tf.feature_column.numeric_column('pct_capm_error', dtype=tf.float32)
    pct_ma_50_volatility = tf.feature_column.numeric_column('pct_ma_50_volatility', dtype=tf.float32)
    stock_price = tf.feature_column.numeric_column('stock_price', dtype=tf.float32)
    pct_strike_variance = tf.feature_column.numeric_column('pct_strike_variance', dtype=tf.float32)
    option_price = tf.feature_column.numeric_column('option_price', dtype=tf.float32)
    pct_implied_volatility = tf.feature_column.numeric_column('pct_implied_volitility', dtype=tf.float32)
    delta = tf.feature_column.numeric_column('delta', dtype=tf.float32)
    gamma = tf.feature_column.numeric_column('gamma', dtype=tf.float32)
    theta = tf.feature_column.numeric_column('theta', dtype=tf.float32)
    vega = tf.feature_column.numeric_column('vega', dtype=tf.float32)
    rho = tf.feature_column.numeric_column('rho', dtype=tf.float32)
    phi = tf.feature_column.numeric_column('phi', dtype=tf.float32)

    feature_columns = [
                tf.feature_column.indicator_column(trade_type),
                years_to_experiation,
                pct_capm_error,
                pct_ma_50_volatility,
                stock_price,
                pct_strike_variance,
                option_price,
                pct_implied_volatility,
                delta,
                gamma,
                theta,
                vega,
                rho,
                phi]

    return feature_columns

def input_fn(data_file, num_epochs, shuffle, batch_size):
    assert tf.gfile.Exists(data_file), ('%s no found. set the --data_dir argument to the correct path.' % data_file)

    def parse_csv(value):
        print('Parsing', data_file)
        columns = tf.decode_csv(value, record_defaults=_CSV_COLUMN_DEFAULTS)
        features = dict(zip(_CSV_COLUMNS, columns))
        labels = tf.one_hot(features.pop('label'), len(LABEL_VOCAB), on_value=1, off_value=0)
        return {'features': features, 'labels': labels}

    dataset = tf.data.TextLineDataset(data_file).skip(1)

    if shuffle:
        #dataset = dataset.shuffle(buffer_size=_NUM_EXAMPLES['train'])
        dataset = dataset.shuffle(buffer_size=100000000)

    dataset = dataset.map(parse_csv, num_parallel_calls=6)
    dataset = dataset.repeat(num_epochs)
    dataset = dataset.batch(batch_size)
    return dataset.make_one_shot_iterator().get_next()

def model_fn(features, feature_columns, labels, mode):
    #input layer
    input_layer = tf.feature_column.input_layer(features, feature_columns)

    #hidden layers
    hidden1 = tf.layers.dense(inputs=input_layer, units=128, activation=tf.nn.relu)
    hidden2 = tf.layers.dense(inputs=hidden1, units=256, activation=tf.nn.relu)
    hidden3 = tf.layers.dense(inputs=hidden2, units=128, activation=tf.nn.relu)

    #logits
    logits = tf.layer.dense(inputs=hidden3, units=len(LABEL_VOCAB), activation=None)

    #compute predictions













if __name__ == "__main__":
  tf.app.run()
