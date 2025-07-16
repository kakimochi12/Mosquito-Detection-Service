from db import create_connection
from cleaning.data_clean import clean_step1, clean_step2
from service.calculate import calculate_service_metrics
from insights.insight_tables import insight_for_data
from collections import defaultdict
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

def extract_data():
    
    # login to database
    username = 'report_user'
    password = 'Report!123'
    host = 'mksol.vn'
    port = '3306'
    database = 'sakai'

    engine = create_connection(host, port, database, username, password)

    sql = 'SELECT * FROM view_mosquito_name_au_year_month_day_sitecode_duration'

    df = pd.read_sql(sql, engine)

    df.to_csv("detect_image_services.csv", index = False)

    return df

def read_data_from_csv():
    df = pd.read_csv("detect_image_services.csv")
    return df

# group data by state, city, and subregion, and calculate:
# image count, total processing duration, revenue, cost, and margin
def geo_sales_summary(df):
    grouped = df.groupby(['state', 'city', 'subregion']).agg(
        image_count = ('photos', 'count'),            # count images per geographic region
        total_duration_ms = ('duration', 'sum')       # sum durations for cost calc
    ).reset_index()

    # calculate financials
    grouped['revenue_aud'] = grouped['image_count'] / 10
    grouped['cost_aud'] = (grouped['total_duration_ms'] / 10000) * 0.25
    grouped['margin_aud'] = grouped['revenue_aud'] - grouped['cost_aud']

    return grouped

def build_geo_tree(df):
    """
    Construct a nested dictionary:
    state to city to subregion to metrics
    where metrics include image count, revenue, and margin.
    """
    # Initialize 3-level nested dictionary using defaultdict
    tree = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))

    # Iterate through each row to populate tree
    for _, row in df.iterrows():
        state = row['state']
        city = row['city']
        subregion = row['subregion']

        # Assign metrics to the nested structure
        tree[state][city][subregion] = {
            'image_count': row['image_count'],
            'revenue_aud': row['revenue_aud'],
            'margin_aud': row['margin_aud']
        }
    print("\n\n ==== HIERARCHICAL TREE ==== \n")

    return dict(tree)  # Convert back to regular dict for export/use


if __name__ == "__main__":
    print("Welcome to Kaiki's Mosquito Analysis!")

    # read our data
    df = read_data_from_csv()

    # clean file
    clean_step1(df)

    # drop ambiguous columns
    clean_step2(df)

    # prints out the contents for service metrics
    calculate_service_metrics(df)
    metrics_df = calculate_service_metrics(df)
    print(metrics_df.head(), "\n\n")

    # print tables giving insights for most active users, most common species, and daily and monthly trends
    insight_for_data(df)

    # Generate the geographic summary table
    geo_summary_df = geo_sales_summary(df)
    print(geo_summary_df.head())


    # build hierarchical tree
    geo_tree = build_geo_tree(geo_summary_df)
    print(geo_tree)


    # build stacked bar chart
    # group Revenue by state and get top 5
    top_states = geo_summary_df.groupby('state').agg(
        total_revenue = ('revenue_aud', 'sum')
    ).sort_values('total_revenue', ascending = False).head(5).reset_index()

    print(df['state'].value_counts()) # so we can see Victoria's value

    #PLOT
    plt.figure(figsize = (10, 4))
    sns.barplot(data=top_states, x='total_revenue', y='state', palette='Blues_d')
    plt.title('Top 5 States by Revenue')
    plt.xlabel('Total Revenue (AUD)')
    plt.ylabel('State')
    plt.tight_layout()
    plt.show()


    # display a heatmap
    # creating a pivot table for heatmap

    pivot_heatmap = geo_summary_df.pivot_table(
        index = 'subregion',
        columns = 'city',
        values='revenue_aud',
        aggfunc='sum',
        fill_value=0
    )

    plt.figure(figsize=(14, 34))
    sns.heatmap(
        pivot_heatmap,
        cmap='YlGnBu',
        linewidths=0.5,
        linecolor='gray',
        annot=False  # set to True if you want exact values shown
    )

    plt.title('Heatmap: Revenue by City and Subregion')
    plt.xlabel('City')
    plt.ylabel('Subregion')
    plt.tight_layout()
    plt.show()


    # BAR CHART: for top 5 users

    # sort and take top 5 users by revenue
    top_users = metrics_df.sort_values(by='revenue_aud', ascending=False).head(5)

    # Plot
    plt.figure(figsize=(10, 6))
    sns.barplot(data=top_users, x='revenue_aud', y='createdby_username', palette='viridis')
    plt.title('Top 5 Users by Revenue')
    plt.xlabel('Revenue (AUD)')
    plt.ylabel('User')
    plt.tight_layout()
    plt.show()

    # Pie chart for species distribution
    # Count top 5 species
    top_species = df['detected_name'].value_counts().head(5)

    # Plot
    plt.figure(figsize=(7, 7))
    plt.pie(top_species, labels=top_species.index, autopct='%1.1f%%', startangle=140)
    plt.title('Top 5 Detected Mosquito Species')
    plt.axis('equal')  # make it a circle
    plt.tight_layout()
    plt.show()

    # LINE CHART: daily number of detections
    # Group by date
    daily_trend = df.groupby('date').size().reset_index(name='detections')

    # Plot
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=daily_trend, x='date', y='detections', marker='o')
    plt.title('Daily Number of Detections')
    plt.xlabel('Date')
    plt.ylabel('Number of Detections')
    plt.tight_layout()
    plt.show()

    # Tree Chart / grouped bar chart: State to City to Subregion analysis
    # We'll use state + city as x-axis, and subregion as hue
    grouped_bar = geo_summary_df.copy()
    grouped_bar['state_city'] = grouped_bar['state'] + ' / ' + grouped_bar['city']

    # Plot
    plt.figure(figsize=(16, 8))
    sns.barplot(data=grouped_bar, x='state_city', y='revenue_aud', hue='subregion')
    plt.title('Revenue by Subregion Grouped by State/City')
    plt.xlabel('State / City')
    plt.ylabel('Revenue (AUD)')
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Subregion', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.show()


    # DESCRIPTIVE STATISTICS (Advanced optional 1)
    # Descriptive stats on geo-level financials
    print(geo_summary_df[['image_count', 'total_duration_ms', 'revenue_aud', 'cost_aud', 'margin_aud']].describe())


    # CORRELATION MATRIX
    # Compute correlations
    corr = geo_summary_df[['image_count', 'total_duration_ms', 'revenue_aud', 'cost_aud', 'margin_aud']].corr()

    # Plot as heatmap
    plt.figure(figsize=(8, 6))
    sns.heatmap(corr, annot=True, cmap='coolwarm', fmt='.2f', square=True)
    plt.title('Correlation Matrix')
    plt.tight_layout()
    plt.show()

    # Bivariate Analysis Scatter plot of duration VS revenue
    plt.figure(figsize=(10, 6))

    # Plot scatter manually
    scatter = sns.scatterplot(
        data=geo_summary_df,
        x='total_duration_ms',
        y='revenue_aud',
        hue='state',
        size='image_count',
        sizes=(50, 300),  # control bubble size range
        alpha=0.7,
        legend=False  # hide the default messy legend
    )

    # Add custom hue legend
    handles_h, labels_h = scatter.get_legend_handles_labels()
    plt.legend(handles=handles_h[1:], title='State', bbox_to_anchor=(1.05, 1), loc='upper left')

    # Add a manual legend for bubble size
    for size in [2500, 5000, 10000, 12500]:
        plt.scatter([], [], s=(size / 50), label=f'{size} images', color='gray', alpha=0.6)

    plt.legend(title='Bubble Size (Image Count)', bbox_to_anchor=(1.05, 0.5), loc='center left')

    plt.title('Bivariate Analysis: Duration vs Revenue')
    plt.xlabel('Total Duration (ms)')
    plt.ylabel('Revenue (AUD)')
    plt.tight_layout()
    plt.show()

    # MULTIVARIATE ANALYSIS
    # Select numeric columns for multivariate comparison
    pair_df = geo_summary_df[['image_count', 'total_duration_ms', 'revenue_aud', 'cost_aud', 'margin_aud']]


    # Create pair plot
    sns.pairplot(pair_df)
    plt.suptitle('Multivariate Analysis: Revenue, Duration, Cost, Margin, Volume', y=1.02)
    plt.show()










    


    