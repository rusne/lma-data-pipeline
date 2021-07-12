
import pandas as pd
import matplotlib.pyplot as plt
# import variables as var


# ______________________________________________________________________________
# VISUALISATION PARAMETERS
# ______________________________________________________________________________


params = {'legend.fontsize': 'xx-small',
          'axes.labelsize': 'xx-small',
          'axes.titlesize': 'x-small',
          'xtick.labelsize': 'xx-small',
          'ytick.labelsize': 'xx-small',
          'axes.edgecolor': 'black',
          'axes.facecolor': 'white',
          'axes.labelcolor': 'black',
          'figure.facecolor': 'white',
          'text.color': 'black',
          'xtick.color': 'black',
          'ytick.color': 'black',
          'legend.edgecolor': 'white',
          'axes.spines.top': False,
          'axes.spines.right': False,
          }
plt.rcParams.update(params)

plt.rc('legend', **{'fontsize': 'xx-small'})
colors = 'viridis'
# colors = 'nipy_spectral'


# ______________________________________________________________________________
# FIGURE 1: Algorithm runtime dependency on the search radius
# ______________________________________________________________________________

data = pd.read_csv('Private_data/results/buffer_stats.csv', sep=';')

title = 'Runtime dependent on the search space radius'
viz1 = data[['Buffer dist., m', 'Search space runtime, s', 'Matching runtime, s']]
viz1.set_index('Buffer dist., m', inplace=True)

fig1 = viz1.plot.line(colormap=colors, legend=True, title=title,
               xlim=(0, 5000),
               ylim=(0, 1000))

print(viz1)
fig1.figure.savefig('results/buffer_stats_fig1.png')

plt.show()

# ______________________________________________________________________________
# FIGURE 2: Algorithm matching success ratio dependency on the search radius
# ______________________________________________________________________________

title = 'Matching success dependent on the search space radius'
viz2 = data[['Buffer dist., m', '% matches']]
viz2.set_index('Buffer dist., m', inplace=True)

fig2 = viz2.plot.line(colormap=colors, legend=True, title=title,
               xticks=[50, 250, 500, 1000, 2500, 5000],
               xlim=(0, 5000),
               ylim=(0, 100))

print(viz2)
fig2.figure.savefig('results/buffer_stats_fig2.png')

plt.show()

# ______________________________________________________________________________
# FIGURE 5: Matching quality per subset
# ______________________________________________________________________________


data = pd.read_csv('Private_data/results/validation_nothresh.csv', sep=';')

title = 'Matching quality'

# data['subset'] = data['match'].str[1]
# data['set'] = data['match'].str[0]

# viz = data[['set', 'subset', 'validity']]
viz = data[['match', 'validity']]

viz['validity'] = viz['validity'].loc[viz['validity'].notna()].astype('int64').apply(str)
viz.fillna('na', inplace=True)


viz = viz.groupby(['match', 'validity'])['validity'].agg('count')

viz = viz.reset_index(name='count')

print(viz)

# colour code
outer_cmap = pd.DataFrame.from_dict({'1a': '#3A5683',
                                       '1b': '#4B6FAA',
                                       '2a': '#2708A0',
                                       '2b': '#340BD5',
                                       '3a': '#6A02A2',
                                       '3b': '#8402CA',
                                       '4a': '#C46BAE',
                                       '4b': '#D08ABF',
                                       '5a': '#AF125A',
                                       '5b': '#DE1774',
                                       '6a': '#B33951',
                                       '6b': '#C9546C',
                                       'na': '#CCCCCC'},
                                      orient='index', columns=['outer_colour'])

inner_cmap = pd.DataFrame.from_dict({'-2': '#FF928B',
                               '-1': '#FEC3A6',
                               '0': '#EFE9AE',
                               '1': '#CDEAC0',
                               '2': '#8DB38B',
                               'na': '#CCCCCC'},
                              orient='index', columns=['inner_colour'])

# assign colours
viz = pd.merge(viz, outer_cmap, left_on='match', right_index=True)
viz = pd.merge(viz, inner_cmap, left_on='validity', right_index=True)
viz.sort_values(by=['match', 'validity'], inplace=True)
# inner = viz.groupby(['subset'])['validity'].agg('count')

outer_viz = viz.groupby(['match', 'outer_colour'])['count'].sum()
outer_viz = outer_viz.reset_index(name='sum')
print(outer_viz)

# NESTED PIE

fig, ax = plt.subplots()
size = 0.3

ax.pie(outer_viz['sum'], radius=1, colors=outer_viz['outer_colour'],
                                  wedgeprops=dict(width=size, edgecolor='w', linewidth=0.25))

ax.pie(viz['count'], radius=1 - size, colors=viz['inner_colour'],
       wedgeprops=dict(width=size, edgecolor='w', linewidth=0.25))
#
ax.set(aspect="equal", title=title)

ax.legend(outer_viz['match'])

plt.show()

# LEGEND PIE

fig, ax = plt.subplots()

inner_leg = viz.groupby(['validity', 'inner_colour'])['count'].sum()
inner_leg = inner_leg.reset_index(name='sum')

ax.pie(inner_leg['sum'], radius=1, colors=inner_leg['inner_colour'],
       wedgeprops=dict(edgecolor='w', linewidth=0.25))
#
ax.set(aspect="equal", title=title)

ax.legend(inner_leg['validity'])

plt.show()

# ______________________________________________________________________________
# FIGURE 7: Number of entities per NACE - EWC
# ______________________________________________________________________________

data = pd.read_excel('Private_data/results/validation_AG.xlsx', dtype=str)

data = data[data['Valid'] == '1']
data = data[['LMA_key', 'LMA_eural', 'KvK_ag']]

data['LMA_eural'] = data['LMA_eural'].str.slice(stop=2)

data = data.groupby(['LMA_eural', 'KvK_ag']).count().reset_index()

data = data.pivot(index='LMA_eural', columns='KvK_ag', values='LMA_key')
data.fillna(0, inplace=True)

# data['Sum'] = data.sum(axis=1)
# data.loc['Sum'] = data.sum()

print(data)

plt.imshow(data, cmap="viridis")

plt.colorbar()
plt.xticks(range(len(data.columns)), data.columns)
plt.yticks(range(len(data.index)), data.index)

for i in range(len(data.columns)):
    for j in range(len(data.index)):
        text = data.loc[data.index[j], data.columns[i]]
        if text == 0:
            text = '-'
        else:
            text = str(int(text))
        plt.text(i, j, text, ha="center", va="center", color="w", fontsize='xx-small')

plt.show()


# ______________________________________________________________________________
# FIGURE 8: Number of entities per EWC - subset
# ______________________________________________________________________________

data = pd.read_excel('Private_data/result.xlsx', dtype=str)

data = data[data['match'] == '0']
data.loc[data['match'].isin(['5a', '5b', '6a', '6b']), ['Confidence']] = 'Low confidence'
data.loc[data['match'].isin(['1a', '2a', '2b', '3a', '3b', '4a', '4b']), ['Confidence']] = 'High confidence'
data.loc[data['match'].isin(['0']), ['Confidence']] = 'Unmatched'

data = data[['LMA_key', 'LMA_eural', 'Confidence']]

euralcodes = pd.DataFrame(data['LMA_eural'].str.split(',').tolist(), index=data['LMA_key']).stack()
euralcodes = euralcodes.reset_index([0, 'LMA_key'])
euralcodes.columns = ['LMA_key', 'LMA_eural']

euralcodes['LMA_eural'] = euralcodes['LMA_eural'].astype(str).str.zfill(6)
euralcodes['LMA_eural'] = euralcodes['LMA_eural'].str.slice(stop=2)
euralcodes.drop_duplicates(inplace=True)
#
data = pd.merge(data[['LMA_key', 'Confidence']], euralcodes, on='LMA_key')

data = data.groupby(['LMA_eural', 'Confidence']).count().unstack()

# print(data)

# data.plot.bar(stacked=True, cmap="viridis")
# plt.show()

data.columns = data.columns.droplevel(0)
print(data.columns)
data['High'] = data['High confidence'] / data.sum(axis=1)
print(data)


plt.imshow(data, cmap="viridis")

plt.colorbar()
plt.xticks(range(len(data.columns)), data.columns)
plt.yticks(range(len(data.index)), data.index)

for i in range(len(data.columns)):
    for j in range(len(data.index)):
        text = data.loc[data.index[j], data.columns[i]]
        if text == 0:
            text = '-'
        else:
            text = str(int(text))
        plt.text(i, j, text, ha="center", va="center", color="w", fontsize='xx-small')

plt.show()
