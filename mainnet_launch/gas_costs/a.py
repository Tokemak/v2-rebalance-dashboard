import streamlit as st
import plotly.graph_objects as go

# Revenue and Expense components
revenue_subscriptions = 3000
revenue_onetime = 1500
expense_rent = -800
expense_payroll = -1200

# Sidebar checkboxes to toggle components
st.sidebar.header("Toggle Components")
show_revenue_subscriptions = st.sidebar.checkbox("Revenue - Subscriptions", value=True)
show_revenue_onetime = st.sidebar.checkbox("Revenue - One-Time", value=True)
show_expense_rent = st.sidebar.checkbox("Expense - Rent", value=True)
show_expense_payroll = st.sidebar.checkbox("Expense - Payroll", value=True)

# Calculate total revenue, total expenses, and profit based on selected components
selected_revenue = (revenue_subscriptions if show_revenue_subscriptions else 0) + \
                   (revenue_onetime if show_revenue_onetime else 0)
selected_expense = (expense_rent if show_expense_rent else 0) + \
                   (expense_payroll if show_expense_payroll else 0)
profit = selected_revenue + selected_expense

# Create a figure
fig = go.Figure()

# Add Revenue components in a stacked bar
if show_revenue_subscriptions:
    fig.add_trace(go.Bar(
        x=["Revenue"],
        y=[revenue_subscriptions],
        name="Revenue - Subscriptions",
        marker_color='gold'
    ))

if show_revenue_onetime:
    fig.add_trace(go.Bar(
        x=["Revenue"],
        y=[revenue_onetime],
        name="Revenue - One-Time",
        marker_color='royalblue'
    ))

# Add Expense components in a separate stacked bar
if show_expense_rent:
    fig.add_trace(go.Bar(
        x=["Expenses"],
        y=[expense_rent],
        name="Expense - Rent",
        marker_color='red'
    ))

if show_expense_payroll:
    fig.add_trace(go.Bar(
        x=["Expenses"],
        y=[expense_payroll],
        name="Expense - Payroll",
        marker_color='green'
    ))

# Add a profit marker
fig.add_trace(go.Scatter(
    x=["Profit"],
    y=[profit],
    name="Profit",
    mode='markers+text',
    text=[f"${profit}"],
    textposition="top center",
    marker=dict(color='white', size=10),
))

# Update layout for the plot
fig.update_layout(
    barmode='stack',
    title="Interactive Bridge Plot: Revenue, Expenses, and Dynamic Profit for Current Month",
    yaxis_title="Amount ($)",
    xaxis_title="",
    template="plotly_dark",
    legend_title="Components",
    showlegend=True,
)

# Display the Plotly figure in Streamlit
st.plotly_chart(fig)
