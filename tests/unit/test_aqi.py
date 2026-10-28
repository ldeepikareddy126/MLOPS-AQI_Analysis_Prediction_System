from aqi_pipeline.transform.aqi import category, sub_index


def test_pm25_sub_index_example():
    assert round(sub_index("PM2.5", 45.0)) == 75
    assert category(sub_index("PM2.5", 45.0))[0] == "Satisfactory"


def test_co_sub_index_example():
    assert round(sub_index("CO", 5.0)) == 137
    assert category(sub_index("CO", 5.0))[0] == "Moderate"


def test_pm10_sub_index_example():
    assert round(sub_index("PM10", 200.0)) == 167
    assert category(sub_index("PM10", 200.0))[0] == "Moderate"


def test_sub_index_gap_handling():
    # Value falling in the 30.0 - 30.1 gap for PM2.5
    val = sub_index("PM2.5", 30.05)
    assert val is not None
    assert round(val) in (50, 51)

    # Value falling in the 50.0 - 51.0 gap for PM10
    val_50_4 = sub_index("PM10", 50.4)
    assert val_50_4 == 50.0
    val_50_6 = sub_index("PM10", 50.6)
    assert val_50_6 == 51.0


def test_final_aqi_calculation():
    from aqi_pipeline.transform.aqi import final_aqi

    aqi_val, dominant = final_aqi({"PM2.5": 250.0, "PM10": 180.0, "NO2": 50.0})
    assert aqi_val == 250.0
    assert dominant == "PM2.5"
    cat_name, cat_num = category(aqi_val)
    assert cat_name == "Poor"
    assert cat_num == 4


def test_final_aqi_requires_three_pollutants_including_particulate():
    from aqi_pipeline.transform.aqi import final_aqi

    assert final_aqi({"PM2.5": 80.0, "NO2": 50.0}) == (None, None)
    assert final_aqi({"NO2": 80.0, "SO2": 50.0, "CO": 25.0}) == (None, None)
    assert final_aqi({"PM10": 100.0, "NO2": 50.0, "SO2": 25.0}) == (100.0, "PM10")
