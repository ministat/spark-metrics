# Import required libraries
import copy
import dash
import datetime as DT
import math
import os
import pandas as pd
import pathlib
import pickle
import plotly.graph_objs as go
from dash.dependencies import Input, Output, State, ClientsideFunction
import dash_core_components as dcc
import dash_html_components as html
import socket
from dataskews import get_data_skews_datatable, load_data_skew_stages, gen_empty_data_skew, load_all, load_data_for_queue

def load_stages(dataDir):
    dic = {}
    listOfDir = os.listdir(dataDir)
    for dt in listOfDir:
        dtDir = dataDir.joinpath(dt)
        if os.path.isdir(dtDir) is False:
            continue
        qDir = os.listdir(dtDir)
        for dq in qDir:
            stagesInfoPath = dtDir.joinpath(dq).joinpath("stages_info.csv")
            if os.path.exists(stagesInfoPath) == False:
                continue
            stageInfoDf = pd.read_csv(stagesInfoPath)
            if dt in dic:
                if dq in dic[dt]:
                    print("Duplication queue '{q}'".format(q=dq))
                else:
                    dic.setdefault(dt, {})[dq] = stageInfoDf
            else:
                dic.setdefault(dt, {})[dq] = stageInfoDf
    return dic

def parse_dataskew_info_save_to_pickle():
    df = load_all(DATA_PATH.joinpath("stageAnalysis"))
    if df.empty == True:
        df = gen_empty_data_skew(queue_selector)
    df.to_pickle(DATASKEW_PICKLE_FILE)

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

DATASKEW_PICKLE_FILE = "dataskews.pickle"

# get relative data folder
PATH = pathlib.Path(__file__).parent
DATA_PATH = PATH.joinpath("data").resolve()
dfDic = load_stages(DATA_PATH.joinpath("stageAnalysis"))
parse_dataskew_info_save_to_pickle()

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
            html.H5("CARMEL internal Analytics"),
            html.H3("Welcome to the CARMEL internal Analytics Dashboard"),
            html.Div(
                id="intro",
                children="Explore the Spark jobs internal insights to help understand the status of application requirement of resources or quick diagnostic of potential issues.",
            ),  
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
            html.Br(),
            html.P("Select queue:", className="control_label"),
            dcc.Dropdown(
                id="queue_selector",
                options=[{"label": i, "value": i} for i in queue_list],
                value=queue_list[0],
                #multi=True,
            ),
        ]
    )

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
                            [
                                html.Div(
                                    [html.H6(id="cpuBoundText"), html.P("No. of CPU bound")],
                                    id="cpuBound",
                                    className="mini_container",
                                ),
                                html.Div(
                                    [html.H6(id="ioBoundText"), html.P("No. of IO bound")],
                                    id="ioBound",
                                    className="mini_container",
                                ),
                                html.Div(
                                    [html.H6(id="memBoundText"), html.P("No. of Mem bound")],
                                    id="memBound",
                                    className="mini_container",
                                ),
                                html.Div(
                                    [html.H6(id="mixBoundText"), html.P("No. of Mix bound")],
                                    id="mixBound",
                                    className="mini_container",
                                ),
                                html.Div(
                                    [html.H6(id="resourceLimitText"), html.P("No. of Resource limit")],
                                    id="resourceLimit",
                                    className="mini_container",
                                ),
                                html.Div(
                                    [html.H6(id="dataSkewText"), html.P("No. of Data skew")],
                                    id="dataSkew",
                                    className="mini_container",
                                ),
                            ],
                            id="info-container",
                            className="row container-display",
                        ),
                        html.Div(
                            [dcc.Graph(id="count_graph")],
                            id="countGraphContainer",
                            className="pretty_container",
                        ),
                    ],
                    id="right-column",
                    className="eight columns",
                ),
            ],
            className="row flex-display",
        ),
        html.H6(html.P("Data skews details:"), className="mini_container"),
        html.Div(id="data_skews_table", className="row pretty_container table"),
        dcc.Loading(
            id="loading-1",
            children=[html.Div(id="loading-output-1")],
            type="circle",
        )
    ],
    id="mainContainer",
    style={"display": "flex", "flex-direction": "column"},
)

@app.callback(
    [Output("data_skews_table", "children"),
     Output("loading-output-1", "children")],
    [Input("queue_selector", "value")],
)
def update_data_skews_table(queue_selector):
    if os.path.exists(DATASKEW_PICKLE_FILE):
        df = pd.read_pickle(DATASKEW_PICKLE_FILE)
        df = load_data_for_queue(df, queue_selector)
    else:
        df = load_data_skew_stages(DATA_PATH.joinpath("stageAnalysis"), queue_selector)
        if df.empty == True:
            df = gen_empty_data_skew(queue_selector)
    return get_data_skews_datatable(df), ''

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
        "RESOURCELIMIT": "rgb(50, 255, 144)",
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
    app.run_server(debug=False, host=host, port=os.getenv('PLOTLY_DASH_SERVER_PORT', 8088))
