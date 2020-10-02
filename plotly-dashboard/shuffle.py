# Import required libraries
import copy
import dash
import datetime as DT
import math
import os
import pandas as pd
import pathlib
import plotly.graph_objs as go
from dash.dependencies import Input, Output, State, ClientsideFunction
import dash_core_components as dcc
import dash_html_components as html
import socket
from dataskews import get_data_skews_datatable, load_data_skew_stages, gen_empty_data_skew

# return dictionary: {date:{queue:stageDataFrame}}
def load_stages(dataDir,tgtFile):
    dic = {}
    listOfDir = os.listdir(dataDir)
    for dt in listOfDir:
        dtDir = dataDir.joinpath(dt)
        if os.path.isdir(dtDir) is False:
            continue
        qDir = os.listdir(dtDir)
        for dq in qDir:
            stagesInfo = dtDir.joinpath(dq).joinpath(tgtFile) #"stages_info.csv")
            if not os.path.exists(stagesInfo):
                continue
            stageInfoDf = pd.read_csv(stagesInfo)
            if dt in dic:
                if dq in dic[dt]:
                    print("Duplication queue '{q}'".format(q=dq))
                else:
                    dic.setdefault(dt, {})[dq] = stageInfoDf
            else:
                dic.setdefault(dt, {})[dq] = stageInfoDf
    return dic

queue_list=[
        "all",
        "hdmi-default",
        "hdmi-reserved",
        "hdmi-fin-high-pri",
        "hdmi-clsfd",
        "hdmi-prodanalyst",
        "hdmi-cac",
        "hdmi-gmo",
        "hdmi-fin",
        "hdmi-pymt",
        "hdmi-data",
        "hdmi-gcx",
        "hdmi-reserved-test"
]
# get relative data folder
PATH = pathlib.Path(__file__).parent
DATA_PATH = PATH.joinpath("data").resolve()
dfDic = load_stages(DATA_PATH.joinpath("stageAnalysis"), "stages_info.csv")
dfShuffleDic = load_stages(DATA_PATH.joinpath("stageAnalysis"), "shuffle_read.csv")

app = dash.Dash(
    __name__, meta_tags=[{"name": "viewport", "content": "width=device-width"}]
)
server = app.server

def description_card():
    """
    :return: A Div containing dashboard title & descriptions.
    """
    return html.Div(
        id="description-card",
        children=[
            #html.H5("CARMEL internal Analytics"),
            html.H3("CARMEL Slow Shuffle Analytics Dashboard"),
        ],  
    )

def generate_control_card():
    return html.Div(
        id="control-card",
        children=[
            html.P("Select Time"),
            dcc.DatePickerRange(
                id="date-picker-selecor",
                start_date=DT.date.today() - DT.timedelta(days=7),
                end_date=DT.date.today(),
                min_date_allowed=DT.date.today() - DT.timedelta(days=90),
                max_date_allowed=DT.date.today(),
                initial_visible_month=DT.date.today(),
            ),
            html.Br(),
            #html.Br(),
            html.P("Select queue:", className="control_label"),
            dcc.Dropdown(
                id="queue_selector",
                options=[{"label": i, "value": i} for i in queue_list],
                value=queue_list[0],
                #multi=True,
            ),
            html.P("Select the max shuffle read distribution:", className="control_label"),
            dcc.RadioItems(id='radio-shuffle-read-max-duration-id',
               options=[
                  {'label': "less than 1 min", "value": "1"},
                  {'label': "1 min ~ 5 min", "value": "5"},
                  {'label': "longer than 5 min", "value": "6"},
                  {'label': "all", "value": "7"},
               ],
               labelClassName="radio__labels",
               inputClassName="radio__input",
               value="7",
               className="radio__group",
            )
            #dcc.RadioItems(id='radio-shuffle-read-max-duration-id'),
        ]
    )

@app.callback(
    Output("slow_shuffle_count_graph", "figure"),
    [Input("queue_selector", "value"),
     Input("radio-shuffle-read-max-duration-id", "value")]
)
def update_graph(queue_selector, max_shuffle_read):
    dates = []
    figData = []
    # dictionary: {queue: [data_array]}
    data = {
          'hdmi-cac': [],
          'hdmi-clsfd': [],
          'hdmi-data': [],
          'hdmi-default': [],
          'hdmi-fin': [],
          'hdmi-fin-high-pri': [],
          'hdmi-gcx': [],
          'hdmi-gmo': [],
          'hdmi-prodanalyst': [],
          'hdmi-pymt': [],
          'hdmi-reserved': [],
          'hdmi-reserved-test': []
    }
    for k,v in dfShuffleDic.items():
        dates.append(k)
        tmpDic = {
          'hdmi-cac': 0,
          'hdmi-clsfd': 0,
          'hdmi-data': 0,
          'hdmi-default': 0,
          'hdmi-fin': 0,
          'hdmi-fin-high-pri': 0,
          'hdmi-gcx': 0,
          'hdmi-gmo': 0,
          'hdmi-prodanalyst': 0,
          'hdmi-pymt': 0,
          'hdmi-reserved': 0,
          'hdmi-reserved-test': 0
        }
        col = 'fetchWaitTime95'
        for queue, qDataframe in v.items():
            if queue == queue_selector or queue_selector == "all":
             if col in qDataframe:
               if max_shuffle_read == "1":
                  tmpDic[queue] = tmpDic[queue] + qDataframe[qDataframe[col] < 60000].count()
               elif max_shuffle_read == "5":
                  tmpDic[queue] = tmpDic[queue] + qDataframe[(qDataframe[col] >= 60000) and (qDataframe[col] < 300000)].count()
               elif max_shuffle_read == "6": 
                  tmpDic[queue] = tmpDic[queue] + qDataframe[qDataframe[col] >= 300000].count()
               else:
                  tmpDic[queue] = tmpDic[queue] + qDataframe.shape[0]
             else:
               tmpDic[queue] = tmpDic[queue] + qDataframe.shape[0]
        for q,qArr in data.items():
            qArr.append(tmpDic[q])
    for q,qArr in data.items():
        figData.append(
            go.Bar(x=dates, y=data[q], name=q)
        )
    return {
        'data': figData,
        'layout': go.Layout(title='Slow shuffle overview')
    }

def generate_queue_id(queue):
    return "{q}-id".format(q=queue)

def generate_queue_id_text(queue):
    return "{q}-text".format(q=generate_queue_id(queue))

def generate_queue_desc(queue):
    return "No. of {q}".format(q=queue)

def generate_summary():
    data = []
    for queue in queue_list:
        if queue == "all":
           continue
        data.append(
           html.Div(
              [html.H6(id=generate_queue_id_text(queue)), html.P(generate_queue_desc(queue))],
              id=generate_queue_id(queue),
              className="mini_container",
           )
        )
    return data

# Create app layout
app.layout = html.Div(
    [
        dcc.Store(id="aggregate_data"),
        # empty Div to trigger javascript file for graph resizing
        html.Div(id="output-clientside"),
        html.Div(
            [
                html.Div(
                    [
                        html.Img(
                            src=app.get_asset_url("adi-logo.png"),
                            id="plotly-image",
                            style={
                                "height": "60px",
                                "width": "auto",
                                "margin-bottom": "25px",
                            },
                        )
                    ],
                    className="one-third column",
                ),
                html.Div(
                    [
                        html.Div(
                            [
                                html.H3(
                                    "Hot Resource Analysis for Spark",
                                    style={"margin-bottom": "0px"},
                                ),
                                html.H5(
                                    "Stages Overview", style={"margin-top": "0px"}
                                ),
                            ]
                        )
                    ],
                    className="one-half column",
                    id="title",
                ),
                html.Div(
                    [
                        html.A(
                            html.Button("Learn More", id="learn-more-button"),
                            href="https://plot.ly/dash/pricing/",
                        )
                    ],
                    className="one-third column",
                    id="button",
                ),
            ],
            id="header",
            className="row flex-display",
            style={"margin-bottom": "25px"},
        ),
        html.Div(
            [
                html.Div(
                    className="pretty_container four columns",
                    id="cross-filter-options",
                    children=[description_card(), generate_control_card()],
                ),
                html.Div(
                    [
                        html.Div(
                            [dcc.Graph(id="slow_shuffle_count_graph")],
                            id="countGraphContainer2",
                            className="pretty_container",
                        ),
                    ],
                    id="right-column",
                    className="ten columns",
                ),
            ],
            className="row flex-display",
        ),
        html.Div(
            [
                html.Div(
                    [
                        html.Div(
                            [
                                html.Div(
                                    [html.H6(id="hdmi-cac-Text"), html.P("Shuf slow hdmi-cac")],
                                    id="hdmi-cac-id",
                                    className="mini_container",
                                ),
                                html.Div(
                                    [html.H6(id="hdmi-clsfd-Text"), html.P("Shuf slow hdmi-clsfd")],
                                    id="hdmi-clsfd-id",
                                    className="mini_container",
                                ),
                                html.Div(
                                    [html.H6(id="hdmi-data-Text"), html.P("Shuf slow hdmi-data")],
                                    id="hdmi-data-id",
                                    className="mini_container",
                                ),
                                html.Div(
                                    [html.H6(id="hdmi-default-Text"), html.P("Shuf slow hdmi-default")],
                                    id="hdmi-default-id",
                                    className="mini_container",
                                ),
                                html.Div(
                                    [html.H6(id="hdmi-fin-Text"), html.P("Shuf slow hdmi-fin")],
                                    id="hdmi-fin-id",
                                    className="mini_container",
                                ),
                                html.Div(
                                    [html.H6(id="hdmi-fin-high-pri-Text"), html.P("Shuf slow hdmi-fin-high-pri")],
                                    id="hdmi-fin-high-pri-id",
                                    className="mini_container",
                                ),
                                html.Div(
                                    [html.H6(id="hdmi-gcx-Text"), html.P("Shuf slow hdmi-gcx")],
                                    id="hdmi-gcx-id",
                                    className="mini_container",
                                ),
                                html.Div(
                                    [html.H6(id="hdmi-gmo-Text"), html.P("Shuf slow hdmi-gmo")],
                                    id="hdmi-gmo-id",
                                    className="mini_container",
                                ),
                                html.Div(
                                    [html.H6(id="hdmi-prodanalyst-Text"), html.P("Shuf slow hdmi-prodanalyst")],
                                    id="hdmi-prodanalyst-id",
                                    className="mini_container",
                                ),
                                html.Div(
                                    [html.H6(id="hdmi-pymt-Text"), html.P("Shuf slow hdmi-pymt")],
                                    id="hdmi-pymt-id",
                                    className="mini_container",
                                ),
                                html.Div(
                                    [html.H6(id="hdmi-reserved-Text"), html.P("Shuf slow hdmi-reserved")],
                                    id="hdmi-reserved-id",
                                    className="mini_container",
                                ),
                                html.Div(
                                    [html.H6(id="hdmi-reserved-test-Text"), html.P("Shuf slow hdmi-reserved-test")],
                                    id="hdmi-reserved-test-id",
                                    className="mini_container",
                                ),
                            ],
                            id="info-container2",
                            className="row container-display",
                        ),
                    ],
                    id="right-column2",
                    className="twelve columns",
                ),
            ],
            className="row flex-display",
        ),
        html.H6(html.P("Data skews details:"), className="mini_container"),
    ],
    id="mainContainer",
    style={"display": "flex", "flex-direction": "column"},
)

@app.callback(
    [
        Output("hdmi-cac-Text", "children"),
        Output("hdmi-clsfd-Text", "children"),
        Output("hdmi-data-Text", "children"),
        Output("hdmi-default-Text", "children"),
        Output("hdmi-fin-Text", "children"),
        Output("hdmi-fin-high-pri-Text", "children"),
        Output("hdmi-gcx-Text", "children"),
        Output("hdmi-gmo-Text", "children"),
        Output("hdmi-prodanalyst-Text", "children"),
        Output("hdmi-pymt-Text", "children"),
        Output("hdmi-reserved-Text", "children"),
        Output("hdmi-reserved-test-Text", "children"),
    ],
    [Input("queue_selector", "value"),
     Input("radio-shuffle-read-max-duration-id", "value")],
)
def update_slow_shuffle_text(queue_selector, max_shuffle_read):
    tmpDic = {
      'hdmi-cac': 0,
      'hdmi-clsfd': 0,
      'hdmi-data': 0,
      'hdmi-default': 0,
      'hdmi-fin': 0,
      'hdmi-fin-high-pri': 0,
      'hdmi-gcx': 0,
      'hdmi-gmo': 0,
      'hdmi-prodanalyst': 0,
      'hdmi-pymt': 0,
      'hdmi-reserved': 0,
      'hdmi-reserved-test': 0
    }
    col = 'fetchWaitTime95'
    for k,v in dfShuffleDic.items():
        for queue, qDataframe in v.items():
          if queue == queue_selector or queue_selector == "all":
             if col in qDataframe:
               if max_shuffle_read == "1":
                  tmpDic[queue] = tmpDic[queue] + qDataframe[qDataframe[col] < 60000].count()
               elif max_shuffle_read == "5":
                  tmpDic[queue] = tmpDic[queue] + qDataframe[(qDataframe[col] >= 60000) and (qDataframe[col] < 300000)].count()
               elif max_shuffle_read == "6": 
                  tmpDic[queue] = tmpDic[queue] + qDataframe[qDataframe[col] >= 300000].count()
               else:
                  tmpDic[queue] = tmpDic[queue] + qDataframe.shape[0]
             else:
               tmpDic[queue] = tmpDic[queue] + qDataframe.shape[0]
    return tmpDic['hdmi-cac'],tmpDic['hdmi-clsfd'],tmpDic['hdmi-data'],tmpDic['hdmi-default'],tmpDic['hdmi-fin'],tmpDic['hdmi-fin-high-pri'],tmpDic['hdmi-gcx'],tmpDic['hdmi-gmo'],tmpDic['hdmi-prodanalyst'],tmpDic['hdmi-pymt'],tmpDic['hdmi-reserved'],tmpDic['hdmi-reserved-test']

@app.callback(
    Output("data_skews_table", "children"),
    [Input("queue_selector", "value")],
)
def update_data_skews_table(queue_selector):
    df = load_data_skew_stages(DATA_PATH.joinpath("stageAnalysis"), queue_selector)
    if df.empty == False:
        return get_data_skews_datatable(df)
    else:
        return get_data_skews_datatable(gen_empty_data_skew(queue_selector))

@app.callback(
    [
        Output("cpuBoundText", "children"),
        Output("ioBoundText", "children"),
        Output("memBoundText", "children"),
        Output("mixBoundText", "children"),
        Output("resourceLimitText", "children"),
        Output("dataSkewText", "children"),
    ],
    [Input("queue_selector", "value")],
)
def update_text(queue_selector):
    cpu = 0
    io = 0
    mem = 0
    dataSkew = 0
    mix = 0
    resourceLimit = 0
    for k,v in dfDic.items():
        for q,qv in v.items():
            if q==queue_selector or queue_selector=="all":
                cpu = cpu + qv['CPUBound'].sum()
                io = io + qv['IOBound'].sum()
                mem = mem + qv['MemBound'].sum()
                mix = mix + qv['MixBound'].sum()
                resourceLimit = resourceLimit + qv['ResourceLimit'].sum()
                dataSkew = dataSkew + qv['dataSkew'].sum()
    return cpu,io,mem,mix,resourceLimit,dataSkew


@app.callback(
    Output("count_graph", "figure"),
    [
        Input("queue_selector", "value"),
    ],
)
def update_stages_info(queue_selector):
    data = []
    dates = []

    cpus = []
    ios = []
    mems = []
    resourceLimits = []
    dataSkews = []
    mixs = []
    co = {  # colors for stages
        "CPU": "#264e86",
        "MEM": "#0074e4",
        "IO": "#74dbef",
        "MIX": "rgb(255, 127, 14)",
        "DATASKEW": "#eff0f4",
        "RESOURCELIMIT": "rgb(150, 72, 144)",
    }
    print(queue_selector)
    for k,v in dfDic.items():
        dates.append(k)
        cpu = 0
        io = 0
        mem = 0
        resourceLimit = 0
        dataSkew = 0
        mix = 0
        for q,qv in v.items():
            if q == queue_selector or queue_selector == "all":
                cpu = cpu + qv['CPUBound'].sum()
                io = io + qv['IOBound'].sum()
                mem = mem + qv['MemBound'].sum()
                resourceLimit = resourceLimit + qv['ResourceLimit'].sum()
                dataSkew = dataSkew + qv['dataSkew'].sum()
                mix = mix + qv['MixBound'].sum()
            cpus.append(cpu)
            ios.append(io)
            mems.append(mem)
            resourceLimits.append(resourceLimit)
            dataSkews.append(dataSkew)
            mixs.append(mix)
    data.append(go.Bar(
            x = dates,
            y = cpus,
            name = "CPU",
            marker=dict(color=co["CPU"])
        ))
    data.append(go.Bar(
            x = dates,
            y = mems,
            name = "MEM",
            marker=dict(color=co["MEM"])
        ))
    data.append(go.Bar(
            x = dates,
            y = ios,
            name = "IO",
            marker=dict(color=co["IO"])
        ))
    data.append(go.Bar(
            x = dates,
            y = dataSkews,
            name = "DATASKEW",
            marker=dict(color=co["DATASKEW"])
        ))
    data.append(go.Bar(
            x = dates,
            y = mixs,
            name = "MIX",
            marker=dict(color=co["MIX"])
        ))
    data.append(go.Bar(
            x = dates,
            y = mixs,
            name = "RESOURCELIMIT",
            marker=dict(color=co["RESOURCELIMIT"])
        ))
    #elif queue_selector is "hdmi-default":
    layout = go.Layout(
        autosize=True,
        barmode="stack",
        margin=dict(l=40, r=25, b=40, t=0, pad=4),
        paper_bgcolor="white",
        plot_bgcolor="white",
    )
    return {"data": data, "layout": layout}

# Main
if __name__ == "__main__":
    host = socket.gethostbyname(socket.gethostname())
    app.run_server(debug=False, host=host, port=8088)
