library(shinydashboard)
library(shiny)
library(dplyr)
library(lubridate)
library(RPostgreSQL)
library(ggplot2)
library(plotly)
library(data.table)
library(shinyjs)

#Setting Up the Driver and Connection to PostgreSql Database of Bizongo
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, host="production-replica.cn3ezjkfi0ji.ap-south-1.rds.amazonaws.com", port='5432' , dbname='bizongo_production', user='rails', password='connectB2B' )

out <- dbGetQuery(con,"select order_items.pretty_id,
                  direct_orders.pretty_id as prettyidofdirect,
                  shipments.tracking_id,
                  shipments.courier_name,
                  transactions.payment_method,
                  ebs_payment_responses.payment_id,
                  admin_users.first_name as firstname,
                  admin_users.first_name as lastname,
                  buyers.name as buyercompanyname,
                  shipments.actual_charges,
                  order_items.delivery_status,
                  order_items.created_at,
                  users.email,
                  users.contact_number,
                  users.id as buyerid,
                  order_items.sold_at,
                  order_items.cst_tax,
                  order_items.discount,
                  order_items.delivery_charges,
                  order_items.extra_delivery_charges,
                  order_items.cod_charges,
                  order_items.marketing_fees,
                  order_items.seller_delivery_charges,
                  order_items.service_tax_percentage,
                  order_items.swachh_tax_percentage,
                  leads.created_at as leadcreatedat,
                  categories.category,
                  categories.subcategory,
                  categories.subsubcategory
                  
                  from order_items
                  left join companies sellers on sellers.id=order_items.company_id
                  left join direct_orders on direct_orders.id=order_items.direct_order_id
                  left join users on users.id=direct_orders.user_id
                  left join companies buyers on buyers.id=users.company_id
                  left join admin_users on admin_users.id=buyers.admin_user_id
                  left join child_products on child_products.id=order_items.child_product_id
                  left join products on products.id=child_products.product_id
                  left join base_products on base_products.id=products.base_product_id
                  left join categories on categories.id=base_products.category_id
                  left join transactions on transactions.direct_order_id=direct_orders.id
                  left join leads on leads.requester_id=users.id
                  left join order_item_shipment_relations on order_item_shipment_relations.order_item_id=order_items.id
                  left join shipments on shipments.id=order_item_shipment_relations.shipment_id
                  left join addresses pickup_addresses on pickup_addresses.id=shipments.pickup_address_id
                  left join direct_order_shipping_addresses on direct_order_shipping_addresses.direct_order_id=direct_orders.id
                  left join addresses shipping_addresses on shipping_addresses.id=direct_order_shipping_addresses.shipping_address_id
                  left join ebs_payment_responses on cast(transactions.id + 1000 as varchar) = ebs_payment_responses.merchant_ref_no
                  
                  
                  where categories.category != 'Plastics' and categories.category != 'Paints & Pigments' and categories.category != 'Polymers' and categories.category != 'Chemicals' 
                  ")
dbDisconnect(con)

con2 <- dbConnect(drv, host="production-replica.cn3ezjkfi0ji.ap-south-1.rds.amazonaws.com", port='5432' , dbname='bizongo_production', user='rails', password='connectB2B' )

lead <- dbGetQuery(con2,"select leads.id,
categories.category,
categories.subcategory,
categories.subsubcategory,
admin_users.first_name as firstname,
admin_users.last_name as lastname,
leads.status,
leads.source,
lead_statuses.created_at as statuscreatedat,
leads.created_at as leadcreatedat

from leads
left join categories on categories.id=leads.category_id
left join admin_users on admin_users.id=leads.admin_user_id
left join lead_statuses on lead_statuses.lead_id=leads.id

                  ")

dbDisconnect(con2)

lead <- lead %>% mutate(
                             weekwise = floor_date(leadcreatedat,"week"),
                             monthwise =  floor_date(leadcreatedat,"month"),
                             daywise = floor_date(leadcreatedat,"day"),
                             completed_at = max(statuscreatedat),
                             completion_time = difftime(completed_at,leadcreatedat,units = "days")
                             )
lead$new_status <- "a"

for(x in 1:nrow(lead)) {
  if (lead$status[x]==0) {
    lead$new_status[x] <- "start"
  } else if (lead$status[x]==7 || lead$status[x]==8) {
    lead$new_status[x] <- "completed"
  } else {
    lead$new_status[x] <- "in progress"
  }
}

organicleads <- lead %>% filter(grepl("organic",source)==TRUE | grepl("Organic",source)==TRUE)


out <- out %>% mutate(adminname = paste0(firstname," ",lastname),
                      weekwise = floor_date(created_at,"week"),
                      monthwise = floor_date(created_at,"month"),
                      daywise = floor_date(created_at,"day")
                      )

#Replace all NULLS with 0
out$cod_charges[is.na(out$cod_charges)] <- 0
out$discount[is.na(out$discount)] <- 0
out$delivery_charges[is.na(out$delivery_charges)] <- 0
out$extra_delivery_charges[is.na(out$extra_delivery_charges)] <- 0
out$marketing_fees[is.na(out$marketing_fees)] <- 0
out$seller_delivery_charges[is.na(out$seller_delivery_charges)] <- 0
out$service_tax_percentage[is.na(out$service_tax_percentage)] <- 0
out$swachh_tax_percentage[is.na(out$swachh_tax_percentage)] <- 0
out$actual_charges[is.na(out$actual_charges)] <- 0

#Add a GMV column and a 'New or Repeat Buyer' to the 'out' data.frame
out$gmv <- 0
out$new_or_repeat <- "a"
out$organic_or_fulfilled <- "b"

#Final Amount Paid By the Buyer
out$gmv <- out$sold_at+out$cst_tax-out$discount+out$delivery_charges+out$extra_delivery_charges+out$cod_charges

#Add a Seller Payable column to the 'out' data.frame
out$seller_payable <- 0

#Seller Payable (2*swachh bharat because of krishi kalyan)
out$seller_payable <- out$gmv - out$delivery_charges - out$extra_delivery_charges - out$cod_charges - out$marketing_fees - out$seller_delivery_charges - ((out$service_tax_percentage/100)*(out$marketing_fees+out$seller_delivery_charges)) - (2*((out$swachh_tax_percentage/100)*(out$marketing_fees+out$seller_delivery_charges))) + out$discount


for (x in 1:nrow(out)) {
  if(grepl("Satyaprakash",out$firstname[x])==TRUE | grepl("Argha",out$firstname[x])==TRUE | grepl("Saket",out$firstname[x])==TRUE | grepl("Shubham",out$firstname[x])==TRUE | grepl("Hersh",out$firstname[x])==TRUE | grepl("Rahil",out$firstname[x])==TRUE | grepl("Abhishek",out$firstname[x])==TRUE | grepl("Aseem",out$firstname[x])==TRUE | grepl("Chetan",out$firstname[x])==TRUE | grepl("Harshant",out$firstname[x])==TRUE | grepl("Vaibhav",out$firstname[x])==TRUE ) {
    out$organic_or_fulfilled[x] <- "CR Team"
  }
  else if (is.na(out$leadcreatedat[x])==TRUE) {
    out$organic_or_fulfilled[x] <- "Organic"
  }
  else {
    out$organic_or_fulfilled[x] <- "CS Team"
  }
}

out <- out %>% select(created_at,delivery_status,organic_or_fulfilled,pretty_id,prettyidofdirect,weekwise,daywise,monthwise,gmv,seller_payable,actual_charges) %>% mutate(margin=(gmv-seller_payable-actual_charges))
out <- unique(out)

#Function to show y labels in a better way in a graph
fancy_scientific <- function(l) {
  # turn in to character string in scientific notation
  l <- format(l, scientific = TRUE)
  # quote the part before the exponent to keep all the digits
  l <- gsub("^(.*)e", "'\\1'e", l)
  # turn the 'e+' into plotmath format
  l <- gsub("e", "%*%10^", l)
  # return this as an expression
  parse(text=l)
}

ui <- dashboardPage(
  dashboardHeader(title = "Bizongo Dashboard"),
  
  dashboardSidebar(
    sidebarMenu(
      menuItem(dateRangeInput('dateRangeInput',
                              label = 'Date range:',
                              start = Sys.Date()-30, end = Sys.Date()
      ),tabName = "daterange"),
      menuItem(selectInput("timeselect","Choose Time Series",c("Weekwise"="weekwise","Daywise"="daywise","Monthwise"="monthwise")),tabName = "timeselector"),
      
      menuItem("Orders",tabName = "orders",icon = icon("dashboard"),selected = TRUE),
      
      menuItem("Leads", tabName = "leads",icon = icon("dashboard")),
      menuItem("Leads Metrics", tabName = "leadsmetrics",icon = icon("dashboard"))
    )
  ),
  
  dashboardBody(
    tabItems(
      tabItem(tabName = "orders",
              fluidPage(
                fluidRow(
                  tabBox(
                    title = "GMV",
                    # The id lets us use input$tabset1 on the server to find the current tab
                    id = "tabset1", height = "500px",width = "1200px",
                    tabPanel("Organic GMV", plotlyOutput("organicgmv")),
                    tabPanel("CR Team GMV", plotlyOutput("clientrelationsgmv")),
                    tabPanel("CS Team GMV", plotlyOutput("clientsuccessgmv")),
                    tabPanel("Returns", plotlyOutput("returnsandcancellationsgmv"))
                  )
                ),
                fluidRow(
                  tabBox(
                    title = "Orders",
                    # The id lets us use input$tabset1 on the server to find the current tab
                    id = "tabset1", height = "500px",width = "1200px",
                    tabPanel("Organic Orders", plotlyOutput("organicorders")),
                    tabPanel("CR Team Orders", plotlyOutput("clientrelationsorders")),
                    tabPanel("CS Team Orders", plotlyOutput("clientsuccessorders")),
                    tabPanel("Returns", plotlyOutput("returnsandcancellationsorders"))
                  )
                ),
                fluidRow(
                  tabBox(
                    title = "Profit Margin",
                    # The id lets us use input$tabset1 on the server to find the current tab
                    id = "tabset1", height = "500px",width = "1200px",
                    tabPanel("Organic Margin", plotlyOutput("organicmargin")),
                    tabPanel("CR Team Margin", plotlyOutput("clientrelationsmargin")),
                    tabPanel("CS Team Margin", plotlyOutput("clientsuccessmargin"))
                  )
                )
              )
      ),
      tabItem(tabName = "leads",
              fluidPage(
                fluidRow (
                    tabBox(
                      title = "Leads",
                      # The id lets us use input$tabset1 on the server to find the current tab
                      id = "tabset1", height = "500px",width = "1200px",
                      tabPanel("Organic Leads", plotlyOutput("organicleads")),
                      tabPanel("Total Leads", plotlyOutput("totalleads"))
                          )
                        )
                      )
             )
      ,
      tabItem(tabName = "leadsmetrics",
              fluidPage(
                fluidRow(
                  valueBoxOutput("totalconversionpercentage"),
                  valueBoxOutput("totalaveragecompletiontime")
                ),
                fluidRow(
                  selectInput("optionselect","Sort According to:",c("Admin"="admin","Category"="category","Subcategory"="subcategory","Subsubcategory"="subsubcategory"))
                ),
                fluidRow (
                  dataTableOutput("conversion_percentage_and_average_completion_time")
                ),
                fluidRow (
                  dataTableOutput("leadstatus")
                )
              )
          )
    )
  )
)

server <- function(input,output) {

  #GMV==============================================================================  
  organicGMV <- reactive({
    organicPlot <- switch(input$timeselect,
                        weekwise = {ggplot(
                                    data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],organic_or_fulfilled=="Organic",delivery_status == 1 |delivery_status == 2 |delivery_status == 4 |delivery_status == 5 |delivery_status == 6 |delivery_status == 7 |delivery_status == 11) %>% 
                                    group_by(weekwise) %>%
                                    summarise(total_gmv=sum(gmv))), 
                                    aes(x=weekwise,y=c(total_gmv),group=1)) +
                                    geom_line() +
                                    scale_y_continuous(labels = fancy_scientific) +
                                    labs(x="Time",y="GMV",title="Organic GMV" )} ,
           
                        daywise = {ggplot(
                                    data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],organic_or_fulfilled=="Organic",delivery_status == 1 |delivery_status == 2 |delivery_status == 4 |delivery_status == 5 |delivery_status == 6 |delivery_status == 7 |delivery_status == 11) %>% 
                                    group_by(daywise) %>%
                                    summarise(total_gmv=sum(gmv))), 
                                    aes(x=daywise,y=total_gmv,group=1)) +
                                    geom_line() +
                                    labs(x="Time",y="GMV",title="Organic GMV" ) },
                        
                        monthwise = {ggplot(
                          data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],organic_or_fulfilled=="Organic",delivery_status == 1 |delivery_status == 2 |delivery_status == 4 |delivery_status == 5 |delivery_status == 6 |delivery_status == 7 |delivery_status == 11) %>% 
                                    group_by(monthwise) %>%
                                    summarise(total_gmv=sum(gmv))), 
                          aes(x=monthwise,y=total_gmv,group=1)) +
                            geom_line() +
                            labs(x="Time",y="GMV",title="Organic GMV" ) }
           
                        )
    organicPlot
  })

  clientRelationsGMV <- reactive({
    clientRelationsPlot <- switch(input$timeselect,
                          weekwise = {ggplot(
                            data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],organic_or_fulfilled=="CR Team",delivery_status == 1 |delivery_status == 2 |delivery_status == 4 |delivery_status == 5 |delivery_status == 6 |delivery_status == 7 |delivery_status == 11) %>% 
                                      group_by(weekwise) %>%
                                      summarise(total_gmv=sum(gmv))), 
                            aes(x=weekwise,y=total_gmv,group=1)) +
                              geom_line() +
                              labs(x="Time",y="GMV",title="CR GMV" )} ,
                          
                          daywise = {ggplot(
                            data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],organic_or_fulfilled=="CR Team",delivery_status == 1 |delivery_status == 2 |delivery_status == 4 |delivery_status == 5 |delivery_status == 6 |delivery_status == 7 |delivery_status == 11) %>% 
                                      group_by(daywise) %>%
                                      summarise(total_gmv=sum(gmv))), 
                            aes(x=daywise,y=total_gmv,group=1)) +
                              geom_line() +
                              labs(x="Time",y="GMV",title="CR GMV" ) },
                          
                          monthwise = {ggplot(
                            data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],organic_or_fulfilled=="CR Team",delivery_status == 1 |delivery_status == 2 |delivery_status == 4 |delivery_status == 5 |delivery_status == 6 |delivery_status == 7 |delivery_status == 11) %>% 
                                      group_by(monthwise) %>%
                                      summarise(total_gmv=sum(gmv))), 
                            aes(x=monthwise,y=total_gmv,group=1)) +
                              geom_line() +
                              labs(x="Time",y="GMV",title="CR GMV" ) }
                          
    )
    clientRelationsPlot
  })
  
  clientSuccessGMV <- reactive({
    clientSuccessPlot <- switch(input$timeselect,
                                  weekwise = {ggplot(
                                    data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],organic_or_fulfilled=="CS Team",delivery_status == 1 |delivery_status == 2 |delivery_status == 4 |delivery_status == 5 |delivery_status == 6 |delivery_status == 7 |delivery_status == 11) %>% 
                                              group_by(weekwise) %>%
                                              summarise(total_gmv=sum(gmv))), 
                                    aes(x=weekwise,y=total_gmv,group=1)) +
                                      geom_line() +
                                      labs(x="Time",y="GMV",title="CS GMV" )} ,
                                  
                                  daywise = {ggplot(
                                    data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],organic_or_fulfilled=="CS Team",delivery_status == 1 |delivery_status == 2 |delivery_status == 4 |delivery_status == 5 |delivery_status == 6 |delivery_status == 7 |delivery_status == 11) %>% 
                                              group_by(daywise) %>%
                                              summarise(total_gmv=sum(gmv))), 
                                    aes(x=daywise,y=total_gmv,group=1)) +
                                      geom_line() +
                                      labs(x="Time",y="GMV",title="CS GMV" ) },
                                
                                monthwise = {ggplot(
                                  data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],organic_or_fulfilled=="CS Team",delivery_status == 1 |delivery_status == 2 |delivery_status == 4 |delivery_status == 5 |delivery_status == 6 |delivery_status == 7 |delivery_status == 11) %>% 
                                            group_by(monthwise) %>%
                                            summarise(total_gmv=sum(gmv))), 
                                  aes(x=monthwise,y=total_gmv,group=1)) +
                                    geom_line() +
                                    labs(x="Time",y="GMV",title="CS GMV" ) }
                                  
    )
    clientSuccessPlot
  })
  
  #Margin==============================================================================
  organicMargin <- reactive({
    organicPlot <- switch(input$timeselect,
                          weekwise = {ggplot(
                            data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],organic_or_fulfilled=="Organic",delivery_status == 1 |delivery_status == 2 |delivery_status == 4 |delivery_status == 5 |delivery_status == 6 |delivery_status == 7 |delivery_status == 11) %>% 
                                      group_by(weekwise) %>%
                                      summarise(total_margin = (sum(margin)/sum(gmv))*100 )), 
                            aes(x=weekwise,y=total_margin,group=1)) +
                              geom_line() +
                              scale_y_continuous(labels = fancy_scientific) +
                              labs(x="Time",y="Margin",title="Organic Margin" )} ,
                          
                          daywise = {ggplot(
                            data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],organic_or_fulfilled=="Organic",delivery_status == 1 |delivery_status == 2 |delivery_status == 4 |delivery_status == 5 |delivery_status == 6 |delivery_status == 7 |delivery_status == 11) %>% 
                                      group_by(daywise) %>%
                                      summarise(total_margin=(sum(margin)/sum(gmv))*100 )), 
                            aes(x=daywise,y=total_margin,group=1)) +
                              geom_line() +
                              labs(x="Time",y="Margin",title="Organic Margin" ) },
                          
                          monthwise = {ggplot(
                            data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],organic_or_fulfilled=="Organic",delivery_status == 1 |delivery_status == 2 |delivery_status == 4 |delivery_status == 5 |delivery_status == 6 |delivery_status == 7 |delivery_status == 11) %>% 
                                      group_by(monthwise) %>%
                                      summarise(total_margin=(sum(margin)/sum(gmv))*100 )), 
                            aes(x=monthwise,y=total_margin,group=1)) +
                              geom_line() +
                              labs(x="Time",y="Margin",title="Organic Margin" ) }
                          
    )
    organicPlot
  })
  
  clientRelationsMargin <- reactive({
    clientRelationsPlot <- switch(input$timeselect,
                                  weekwise = {ggplot(
                                    data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],organic_or_fulfilled=="CR Team",delivery_status == 1 |delivery_status == 2 |delivery_status == 4 |delivery_status == 5 |delivery_status == 6 |delivery_status == 7 |delivery_status == 11) %>% 
                                              group_by(weekwise) %>%
                                              summarise(total_margin=(sum(margin)/sum(gmv))*100  )), 
                                    aes(x=weekwise,y=c(total_margin),group=1)) +
                                      geom_line() +
                                      scale_y_continuous(labels = fancy_scientific) +
                                      labs(x="Time",y="Margin",title="CR Margin" )} ,
                                  
                                  daywise = {ggplot(
                                    data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],organic_or_fulfilled=="CR Team",delivery_status == 1 |delivery_status == 2 |delivery_status == 4 |delivery_status == 5 |delivery_status == 6 |delivery_status == 7 |delivery_status == 11) %>% 
                                              group_by(daywise) %>%
                                              summarise(total_margin=(sum(margin)/sum(gmv))*100  )), 
                                    aes(x=daywise,y=total_margin,group=1)) +
                                      geom_line() +
                                      labs(x="Time",y="Margin",title="CR Margin" ) },
                                  
                                  monthwise = {ggplot(
                                    data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],organic_or_fulfilled=="CR Team",delivery_status == 1 |delivery_status == 2 |delivery_status == 4 |delivery_status == 5 |delivery_status == 6 |delivery_status == 7 |delivery_status == 11) %>% 
                                              group_by(monthwise) %>%
                                              summarise(total_margin=(sum(margin)/sum(gmv))*100  )), 
                                    aes(x=monthwise,y=total_margin,group=1)) +
                                      geom_line() +
                                      labs(x="Time",y="Margin",title="CR Margin" ) }
                                  
                                  
    )
    clientRelationsPlot
  })
  
  clientSuccessMargin <- reactive({
    clientSuccessPlot <- switch(input$timeselect,
                                weekwise = {ggplot(
                                  data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],organic_or_fulfilled=="CS Team",delivery_status == 1 |delivery_status == 2 |delivery_status == 4 |delivery_status == 5 |delivery_status == 6 |delivery_status == 7 |delivery_status == 11) %>% 
                                            group_by(weekwise) %>%
                                            summarise(total_margin=(sum(margin)/sum(gmv))*100  )), 
                                  aes(x=weekwise,y=c(total_margin),group=1)) +
                                    geom_line() +
                                    scale_y_continuous(labels = fancy_scientific) +
                                    labs(x="Time",y="Margin",title="CS Margin" )} ,
                                
                                daywise = {ggplot(
                                  data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],organic_or_fulfilled=="CS Team",delivery_status == 1 |delivery_status == 2 |delivery_status == 4 |delivery_status == 5 |delivery_status == 6 |delivery_status == 7 |delivery_status == 11) %>% 
                                            group_by(daywise) %>%
                                            summarise(total_margin=(sum(margin)/sum(gmv))*100  )), 
                                  aes(x=daywise,y=total_margin,group=1)) +
                                    geom_line() +
                                    labs(x="Time",y="Margin",title="CS Margin" ) },
                                
                                monthwise = {ggplot(
                                  data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],organic_or_fulfilled=="CS Team",delivery_status == 1 |delivery_status == 2 |delivery_status == 4 |delivery_status == 5 |delivery_status == 6 |delivery_status == 7 |delivery_status == 11) %>% 
                                            group_by(monthwise) %>%
                                            summarise(total_margin=(sum(margin)/sum(gmv))*100  )), 
                                  aes(x=monthwise,y=total_margin,group=1)) +
                                    geom_line() +
                                    labs(x="Time",y="Margin",title="CS Margin" ) }
                                
                                
    )
    clientSuccessPlot
  })
  
  #Number of Orders==============================================================================
  organicOrders <- reactive({
    organicPlot <- switch(input$timeselect,
                          weekwise = {ggplot(
                            data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],organic_or_fulfilled=="Organic",delivery_status == 1 |delivery_status == 2 |delivery_status == 4 |delivery_status == 5 |delivery_status == 6 |delivery_status == 7 |delivery_status == 11) %>% 
                                      group_by(weekwise) %>%
                                      summarise(number_of_orders=n_distinct(prettyidofdirect))), 
                            aes(x=weekwise,y=number_of_orders,group=1)) +
                              geom_line() +
                              labs(x="Time",y="Orders",title="Organic Orders" )} ,
                          
                          daywise = {ggplot(
                            data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],organic_or_fulfilled=="Organic",delivery_status == 1 |delivery_status == 2 |delivery_status == 4 |delivery_status == 5 |delivery_status == 6 |delivery_status == 7 |delivery_status == 11) %>% 
                                      group_by(daywise) %>%
                                      summarise(number_of_orders=n_distinct(prettyidofdirect))), 
                            aes(x=daywise,y=number_of_orders,group=1)) +
                              geom_line() +
                              labs(x="Time",y="Orders",title="Organic Orders" ) },
                          
                          monthwise = {ggplot(
                            data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],organic_or_fulfilled=="Organic",delivery_status == 1 |delivery_status == 2 |delivery_status == 4 |delivery_status == 5 |delivery_status == 6 |delivery_status == 7 |delivery_status == 11) %>% 
                                      group_by(monthwise) %>%
                                      summarise(number_of_orders=n_distinct(prettyidofdirect))), 
                            aes(x=monthwise,y=number_of_orders,group=1)) +
                              geom_line() +
                              labs(x="Time",y="Orders",title="Organic Orders" ) }
                          
    )
    organicPlot
  })
  
  clientRelationsOrders <- reactive({
    clientRelationsPlot <- switch(input$timeselect,
                                  weekwise = {ggplot(
                                    data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],organic_or_fulfilled=="CR Team",delivery_status == 1 |delivery_status == 2 |delivery_status == 4 |delivery_status == 5 |delivery_status == 6 |delivery_status == 7 |delivery_status == 11) %>% 
                                              group_by(weekwise) %>%
                                              summarise(number_of_orders=n_distinct(prettyidofdirect))), 
                                    aes(x=weekwise,y=number_of_orders,group=1)) +
                                      geom_line() +
                                      labs(x="Time",y="Orders",title="CR Orders" )} ,
                                  
                                  daywise = {ggplot(
                                    data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],organic_or_fulfilled=="CR Team",delivery_status == 1 |delivery_status == 2 |delivery_status == 4 |delivery_status == 5 |delivery_status == 6 |delivery_status == 7 |delivery_status == 11) %>% 
                                              group_by(daywise) %>%
                                              summarise(number_of_orders=n_distinct(prettyidofdirect))), 
                                    aes(x=daywise,y=number_of_orders,group=1)) +
                                      geom_line() +
                                      labs(x="Time",y="Orders",title="CR Orders" ) },
                                  
                                  monthwise = {ggplot(
                                    data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],organic_or_fulfilled=="CR Team",delivery_status == 1 |delivery_status == 2 |delivery_status == 4 |delivery_status == 5 |delivery_status == 6 |delivery_status == 7 |delivery_status == 11) %>% 
                                              group_by(monthwise) %>%
                                              summarise(number_of_orders=n_distinct(prettyidofdirect))), 
                                    aes(x=monthwise,y=number_of_orders,group=1)) +
                                      geom_line() +
                                      labs(x="Time",y="Orders",title="CR Orders" ) }
                                  
    )
    clientRelationsPlot
  })
  
  clientSuccessOrders <- reactive({
    clientSuccessPlot <- switch(input$timeselect,
                                weekwise = {ggplot(
                                  data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],organic_or_fulfilled=="CS Team",delivery_status == 1 |delivery_status == 2 |delivery_status == 4 |delivery_status == 5 |delivery_status == 6 |delivery_status == 7 |delivery_status == 11) %>% 
                                            group_by(weekwise) %>%
                                            summarise(number_of_orders=n_distinct(prettyidofdirect))), 
                                  aes(x=weekwise,y=number_of_orders,group=1)) +
                                    geom_line() +
                                    labs(x="Time",y="Orders",title="CS Orders" )} ,
                                
                                daywise = {ggplot(
                                  data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],organic_or_fulfilled=="CS Team",delivery_status == 1 |delivery_status == 2 |delivery_status == 4 |delivery_status == 5 |delivery_status == 6 |delivery_status == 7 |delivery_status == 11) %>% 
                                            group_by(daywise) %>%
                                            summarise(number_of_orders=n_distinct(prettyidofdirect))), 
                                  aes(x=daywise,y=number_of_orders,group=1)) +
                                    geom_line() +
                                    labs(x="Time",y="Orders",title="CS Orders" ) },
                                
                                monthwise = {ggplot(
                                  data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],organic_or_fulfilled=="CS Team",delivery_status == 1 |delivery_status == 2 |delivery_status == 4 |delivery_status == 5 |delivery_status == 6 |delivery_status == 7 |delivery_status == 11) %>% 
                                            group_by(monthwise) %>%
                                            summarise(number_of_orders=n_distinct(prettyidofdirect))), 
                                  aes(x=monthwise,y=number_of_orders,group=1)) +
                                    geom_line() +
                                    labs(x="Time",y="Orders",title="CS Orders" ) }
                                
    )
    clientSuccessPlot
  })
  
  
  #Leads==============================================================================
  organicLeads <- reactive({
    organicLeadsPlot <- switch(input$timeselect,
                                weekwise = {ggplot(
                                  data = (organicleads %>% filter(leadcreatedat>=input$dateRangeInput[1] & leadcreatedat<=input$dateRangeInput[2]) %>% 
                                            group_by(weekwise) %>%
                                            summarise(number_of_leads=n_distinct(id))), 
                                  aes(x=weekwise,y=number_of_leads,group=1)) +
                                    geom_line() +
                                    labs(x="Time",y="Leads",title="Organic Leads" )} ,
                                
                               daywise = {ggplot(
                                 data = (organicleads %>% filter(leadcreatedat>=input$dateRangeInput[1] & leadcreatedat<=input$dateRangeInput[2]) %>% 
                                           group_by(daywise) %>%
                                           summarise(number_of_leads=n_distinct(id))), 
                                 aes(x=daywise,y=number_of_leads,group=1)) +
                                   geom_line() +
                                   labs(x="Time",y="Leads",title="Organic Leads" )} ,
                                
                               monthwise = {ggplot(
                                 data = (organicleads %>% filter(leadcreatedat>=input$dateRangeInput[1] & leadcreatedat<=input$dateRangeInput[2]) %>% 
                                           group_by(monthwise) %>%
                                           summarise(number_of_leads=n_distinct(id))), 
                                 aes(x=monthwise,y=number_of_leads,group=1)) +
                                   geom_line() +
                                   labs(x="Time",y="Leads",title="Organic Leads" )}
                                
    )
    organicLeadsPlot
  })
  
  totalLeads <- reactive({
    totalLeadsPlot <- switch(input$timeselect,
                               weekwise = {ggplot(
                                 data = (lead %>% filter(leadcreatedat>=input$dateRangeInput[1] & leadcreatedat<=input$dateRangeInput[2]) %>% 
                                           group_by(weekwise) %>%
                                           summarise(number_of_leads=n_distinct(id))), 
                                 aes(x=weekwise,y=number_of_leads,group=1)) +
                                   geom_line() +
                                   labs(x="Time",y="Leads",title="Total Leads" )} ,
                               
                               daywise = {ggplot(
                                 data = (lead %>% filter(leadcreatedat>=input$dateRangeInput[1] & leadcreatedat<=input$dateRangeInput[2]) %>% 
                                           group_by(daywise) %>%
                                           summarise(number_of_leads=n_distinct(id))), 
                                 aes(x=daywise,y=number_of_leads,group=1)) +
                                   geom_line() +
                                   labs(x="Time",y="Leads",title="Total Leads" )} ,
                               
                               monthwise = {ggplot(
                                 data = (lead %>% filter(leadcreatedat>=input$dateRangeInput[1] & leadcreatedat<=input$dateRangeInput[2]) %>% 
                                           group_by(monthwise) %>%
                                           summarise(number_of_leads=n_distinct(id))), 
                                 aes(x=monthwise,y=number_of_leads,group=1)) +
                                   geom_line() +
                                   labs(x="Time",y="Leads",title="Total Leads" )}
                               
    )
    totalLeadsPlot
  })
  
  #Leads Metrics==============================================================================
  
  conversionPercentageAndCompletionTime <- reactive({
                              
    
    convperc <- switch(input$optionselect,
                      
                      admin = {lead %>% filter(statuscreatedat>=input$dateRangeInput[1] & statuscreatedat<=input$dateRangeInput[2],status==7 | status==8) %>% group_by(firstname,lastname) %>% summarise(Conversion_Percentage= paste0( round(( uniqueN(id[status==7])/(uniqueN(id[status==7])+uniqueN(id[status==8])) )*100,digits=2),"%" ), Average_Completion_Time= round( mean(completion_time),digits=2)  )},
                      
                      category = {lead %>% filter(statuscreatedat>=input$dateRangeInput[1] & statuscreatedat<=input$dateRangeInput[2],status==7 | status==8) %>% group_by(category) %>% summarise(Conversion_Percentage= paste0( round(( uniqueN(id[status==7])/(uniqueN(id[status==7])+uniqueN(id[status==8])) )*100,digits=2),"%" ),  Average_Completion_Time= round( mean(completion_time),digits=2) )},
                      
                      subcategory = {lead %>% filter(statuscreatedat>=input$dateRangeInput[1] & statuscreatedat<=input$dateRangeInput[2],status==7 | status==8) %>% group_by(subcategory) %>% summarise(Conversion_Percentage= paste0( round(( uniqueN(id[status==7])/(uniqueN(id[status==7])+uniqueN(id[status==8])) )*100,digits=2),"%" ),  Average_Completion_Time= round( mean(completion_time),digits=2) )},
                      
                      subsubcategory = {lead %>% filter(statuscreatedat>=input$dateRangeInput[1] & statuscreatedat<=input$dateRangeInput[2],status==7 | status==8) %>% group_by(subsubcategory) %>% summarise(Conversion_Percentage= paste0( round(( uniqueN(id[status==7])/(uniqueN(id[status==7])+uniqueN(id[status==8])) )*100,digits=2),"%" ),  Average_Completion_Time= round( mean(completion_time),digits=2) )}
    )
    
    convperc
                      })
  
  leadStatus <- reactive({
    
    
    status <- switch(input$optionselect,
                       
                       admin = {lead %>% filter(leadcreatedat>=input$dateRangeInput[1] & leadcreatedat<=input$dateRangeInput[2]) %>% group_by(firstname,lastname) %>% summarise(Start=uniqueN(id[new_status=="start"]), In_Progress=uniqueN(id[new_status=="in progress"]), Ordered=uniqueN(id[status==7]) ,Closed=uniqueN(id[status==8]) )},
                       
                       category = {lead %>% filter(leadcreatedat>=input$dateRangeInput[1] & leadcreatedat<=input$dateRangeInput[2]) %>% group_by(category) %>% summarise(Start=uniqueN(id[new_status=="start"]), In_Progress=uniqueN(id[new_status=="in progress"]), Ordered=uniqueN(id[status==7]), Closed=uniqueN(id[status==8]) )},
                       
                       subcategory = {lead %>% filter(leadcreatedat>=input$dateRangeInput[1] & leadcreatedat<=input$dateRangeInput[2]) %>% group_by(subcategory) %>% summarise(Start=uniqueN(id[new_status=="start"]), In_Progress=uniqueN(id[new_status=="in progress"]), Ordered=uniqueN(id[status==7]), Closed=uniqueN(id[status==8]) )},
                       
                       subsubcategory = {lead %>% filter(leadcreatedat>=input$dateRangeInput[1] & leadcreatedat<=input$dateRangeInput[2]) %>% group_by(subsubcategory) %>% summarise(Start=uniqueN(id[new_status=="start"]), In_Progress=uniqueN(id[new_status=="in progress"]), Ordered=uniqueN(id[status==7]), Closed=uniqueN(id[status==8]) )}
    )
    
    status
  })
  
  totalConversionPercentage <- reactive({
    paste0( round( ( length( (lead %>% filter(leadcreatedat>=input$dateRangeInput[1] & leadcreatedat<=input$dateRangeInput[2],status==7))$id )/(length( (lead %>% filter(leadcreatedat>=input$dateRangeInput[1] & leadcreatedat<=input$dateRangeInput[2],status==7))$id )+length( (lead %>% filter(leadcreatedat>=input$dateRangeInput[1] & leadcreatedat<=input$dateRangeInput[2],status==8))$id )) )*100 ),"%" )
  })
  
  totalAverageCompletionTime <- reactive({
    round( ( lead %>% filter(leadcreatedat>=input$dateRangeInput[1] & leadcreatedat<=input$dateRangeInput[2],status==7 | status==8) %>% summarise(avgtime=mean(completion_time)) )$avgtime, digits = 2)
  })
  
  #Return and Cancellations==============================================================================
  
  returnsAndCancellationsGMV <- reactive({
    returnsPlot <- switch(input$timeselect,
                          weekwise = {ggplot(
                            data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],delivery_status == 3 |delivery_status == 8 |delivery_status == 9) %>% 
                                      group_by(weekwise,delivery_status) %>%
                                      summarise(total_gmv=sum(gmv))), 
                            aes(x=weekwise,y=total_gmv,color=factor(delivery_status))) +
                              geom_line() +
                              scale_y_continuous(labels = fancy_scientific) +
                              labs(x="Time",y="GMV",title="Returned GMV" )} ,
                          
                          daywise = {ggplot(
                            data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],delivery_status == 3 |delivery_status == 8 |delivery_status == 9) %>% 
                                      group_by(daywise,delivery_status) %>%
                                      summarise(total_gmv=sum(gmv))), 
                            aes(x=daywise,y=total_gmv,color=factor(delivery_status))) +
                              geom_line() +
                              labs(x="Time",y="GMV",title="Returned GMV" ) },
                          
                          monthwise = {ggplot(
                            data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],delivery_status == 3 |delivery_status == 8 |delivery_status == 9) %>% 
                                      group_by(monthwise,delivery_status) %>%
                                      summarise(total_gmv=sum(gmv))), 
                            aes(x=monthwise,y=total_gmv,color=factor(delivery_status))) +
                              geom_line() +
                              labs(x="Time",y="GMV",title="Returned GMV" ) }
                          
    )
    returnsPlot
  })
  
  returnsAndCancellationsOrders <- reactive({
    returnsPlot <- switch(input$timeselect,
                          weekwise = {ggplot(
                            data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],organic_or_fulfilled=="Organic",delivery_status == 3 |delivery_status == 8 |delivery_status == 9) %>% 
                                      group_by(weekwise,delivery_status) %>%
                                      summarise(number_of_orders=n_distinct(prettyidofdirect))), 
                            aes(x=weekwise,y=number_of_orders,color=factor(delivery_status),group=1)) +
                              geom_line() +
                              labs(x="Time",y="Orders",title="Returned Orders" )} ,
                          
                          daywise = {ggplot(
                            data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],organic_or_fulfilled=="Organic",delivery_status == 3 |delivery_status == 8 |delivery_status == 9) %>% 
                                      group_by(daywise,delivery_status) %>%
                                      summarise(number_of_orders=n_distinct(prettyidofdirect))), 
                            aes(x=daywise,y=number_of_orders,color=factor(delivery_status),group=1)) +
                              geom_line() +
                              labs(x="Time",y="Orders",title="Returned Orders" ) },
                          
                          monthwise = {ggplot(
                            data = (out %>% filter(created_at>=input$dateRangeInput[1] & created_at<=input$dateRangeInput[2],organic_or_fulfilled=="Organic",delivery_status == 3 |delivery_status == 8 |delivery_status == 9 ) %>% 
                                      group_by(monthwise,delivery_status) %>%
                                      summarise(number_of_orders=n_distinct(prettyidofdirect))), 
                            aes(x=monthwise,y=number_of_orders,color=factor(delivery_status),group=1)) +
                              geom_line() +
                              labs(x="Time",y="Orders",title="Returned Orders" ) }
                          
    )
    returnsPlot
  })
  
  #Render Sections==============================================================================
    output$organicgmv <- renderPlotly(organicGMV())
    output$clientrelationsgmv <- renderPlotly(clientRelationsGMV())
    output$clientsuccessgmv <- renderPlotly(clientSuccessGMV())
    output$returnsandcancellationsgmv <- renderPlotly(returnsAndCancellationsGMV())
    
    output$organicorders <- renderPlotly(organicOrders())
    output$clientrelationsorders <- renderPlotly(clientRelationsOrders())
    output$clientsuccessorders <- renderPlotly(clientSuccessOrders())
    output$returnsandcancellationsorders <- renderPlotly(returnsAndCancellationsOrders())
    
    output$organicmargin <- renderPlotly(organicMargin())
    output$clientrelationsmargin <- renderPlotly(clientRelationsMargin())
    output$clientsuccessmargin <- renderPlotly(clientSuccessMargin())
    
    output$organicleads <- renderPlotly(organicLeads())
    output$totalleads <- renderPlotly(totalLeads())
    
    output$conversion_percentage_and_average_completion_time <- renderDataTable({conversionPercentageAndCompletionTime()})
    output$leadstatus <- renderDataTable({leadStatus()})
    
    output$totalconversionpercentage <- renderValueBox({valueBox( totalConversionPercentage(), "Conversion %")})
    output$totalaveragecompletiontime <- renderValueBox({valueBox( totalAverageCompletionTime(), "Avg Completion Time(Days)",color = "lime")})
}

shinyApp(ui,server)
