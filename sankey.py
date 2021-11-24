# a function that converts pandas dataframe into a sankey

import plotly.graph_objects as go
import pandas as pd
import matplotlib.pyplot as plt


def draw_sankey(df, scattered=False, title_text="Sankey Diagram"):

    # flatten sankey
    if scattered:
        for col in range(len(df.columns) - 3):
            col += 1
            df[df.columns[col]] = df[df.columns[col]] + df.index.astype(str)
    flat = []
    for i in range(len(df.columns) - 2):
        subdf = df[[df.columns[i], df.columns[i + 1], 'amount']]
        subdf = subdf.rename(columns={df.columns[i]: 'source',
                                      df.columns[i + 1]: 'target'})
        if scattered:
            subdf['start_node'] = df[df.columns[1]]
        else:
            subdf['start_node'] = df[df.columns[0]]
        flat.append(subdf)
    flat = pd.concat(flat)

    nodes = list(flat['source']) + list(flat['target'])
    nodes = dict.fromkeys(nodes)
    c = 0
    for node in nodes.keys():
        nodes[node] = c
        c += 1

    cmap = plt.cm.get_cmap('Spectral')
    color_map = dict()
    start_nodes = list(flat['start_node'].drop_duplicates())
    step = 1.0 / len(start_nodes)
    node_colors = []
    for i in range(len(start_nodes)):
        rgba = cmap(i * step)
        rgba = tuple(int((255 * x)) for x in rgba[0:3]) + (0.8,)
        rgba = 'rgba' + str(rgba)
        color_map[start_nodes[i]] = rgba
        node_colors.append(rgba)

    # color_map = {'Afvalbeheer / secundair afval': 'rgba(255, 230, 0, 0.3)',
    #              'Buiten MRA': 'rgba(255, 145, 0, 0.3)',
    #              'Industrie, opslag en handel': 'rgba(229, 0, 130, 0.3)',
    #              'Overige': 'rgba(160, 0, 120, 0.3)',
    #              'Route Inzameling': 'rgba(0, 70, 153, 0.3)',
    #              'Dienstensector, overheid en zorg': 'rgba(41, 120, 142, 0.3)',
    #              'Bouwnijverheid': 'rgba(0, 157, 230, 0.3)',
    #              }

    start_nodes = list(flat['start_node'].drop_duplicates())
    node_colors = []
    for i in range(len(start_nodes)):
        rgba = color_map[start_nodes[i]]
        node_colors.append(rgba)

    # print(start_nodes)
    # print(color_map)

    sources = []
    targets = []
    values = []
    colors = []
    for index, row in flat.iterrows():
        sources.append(nodes[row['source']])
        targets.append(nodes[row['target']])
        values.append(row['amount'])
        colors.append(color_map[row['start_node']])

    fig = go.Figure(data=[go.Sankey(
                          # Define nodes
                          node=dict(pad=15,
                                    thickness=15,
                                    line=dict(color="black", width=0.01),
                                    label=list(nodes.keys()),
                                    # customdata=list(nodes.keys()),
                                    # color=node_colors
                                    color="black"
                                    ),
                          # Add links
                          link=dict(source=sources,
                                    target=targets,
                                    value=values,
                                    color=colors
                                    # label=[]
                                    ))])

    fig.update_layout(title_text=title_text,
                      font=dict(size = 12, color = 'black', family = 'Open Sans'),
                      # font=dict(size = 10, color = 'black', family = 'Courier New'),
                      # plot_bgcolor='#F2F2F3',
                      # paper_bgcolor='#F2F2F3',
                      # plot_bgcolor='#070D22',
                      # paper_bgcolor='#070D22',
                      )
    fig.show()


def draw_circular_sankey(df, title_text="Sankey Diagram"):

        nodes = list(df['source']) + list(df['target'])
        nodes = dict.fromkeys(nodes)
        c = 0
        for node in nodes.keys():
            nodes[node] = c
            c += 1

        # cmap = plt.cm.get_cmap('Spectral')
        # color_map = dict()
        # start_nodes = list(df['colour'].drop_duplicates())
        # step = 1.0 / len(start_nodes)
        # node_colors = []
        # for i in range(len(start_nodes)):
        #     rgba = cmap(i * step)
        #     rgba = tuple(int((255 * x)) for x in rgba[0:3]) + (0.5,)
        #     rgba = 'rgba' + str(rgba)
        #     color_map[start_nodes[i]] = rgba
        #     node_colors.append(rgba)

        # start_nodes = list(df['colour'].drop_duplicates())
        # node_colors = []
        # for i in range(len(start_nodes)):
        #     rgba = color_map[start_nodes[i]]
        #     node_colors.append(rgba)

        # print(start_nodes)
        # print(color_map)

        sources = []
        targets = []
        values = []
        colors = []
        labels = []
        for index, row in df.iterrows():
            sources.append(nodes[row['source']])
            targets.append(nodes[row['target']])
            values.append(row['amount'])
            colors.append(row['colour'])
            labels.append(row['tag'])

        fig = go.Figure(data=[go.Sankey(
                              # Define nodes
                              node=dict(pad=100,
                                        thickness=15,
                                        line=dict(color="black", width=0.01),
                                        label=list(nodes.keys()),
                                        # color=node_colors
                                        color="black"
                                        ),
                              # Add links
                              link=dict(source=sources,
                                        target=targets,
                                        value=values,
                                        color=colors,
                                        label=labels
                                        ))])

        fig.update_layout(title_text=title_text,
                          font=dict(size = 12, color = 'white', family = "Arial"),

                          # plot_bgcolor='#F2F2F3',  # off white
                          # paper_bgcolor='#F2F2F3',  # off white
                          # plot_bgcolor='#262626',  # black of the maps
                          # paper_bgcolor='#262626'  # black of the maps
                          plot_bgcolor='#070D22',  # website blue
                          paper_bgcolor='#070D22',  # website blue
                          )
        fig.show()


# test_sankey = pd.read_excel('results/testsankey.xlsx')
# test_sankey = test_sankey.groupby(['activity_group_name', 'status', 'VerwerkingsMethode'], as_index=False)['amount'].sum()
# draw_sankey(test_sankey)
#
# circular_sankey = pd.read_excel(f'Private_data/input-output/{filename}.xlsx', sheet_name=sheet)
# title = ''
# draw_circular_sankey(circular_sankey, title_text=title)
