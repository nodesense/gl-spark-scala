var faker = require('faker');
const jsonfile = require('jsonfile')
const path = require("path")
var mkdirp = require('mkdirp');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

var randomName = faker.name.findName(); // Rowan Nikolaus
var randomEmail = faker.internet.email(); // Kassandra.Haley@erich.biz
var randomCard = faker.helpers.createCard(); // random contact card containing many properties

console.log(randomName, randomEmail, randomCard)

console.log(faker.random.uuid())

console.log(faker.commerce.price())

console.log(faker.commerce.productName())

console.log(faker.commerce.product())

console.log(faker.commerce.product())

console.log(faker.commerce.productMaterial())


console.log(faker.commerce.department())


function generateCustomers(n) {
    let customers = []
    let addresses = []
    for (let i = 0; i < n; i++) {
        let cust = faker.helpers.contextualCard()
        let customer = {
            CustomerID:  faker.random.uuid(),
            Name: cust.name,
            UserName: cust.username,
            Email: cust.email,
            Phone: cust.phone,
            Website: cust.website,
            Company: cust.company.name
        }

        let address = {
            CustomerID: customer.CustomerID,
            Street: cust.address.street,
            Suite: cust.address.suite,
            City: cust.address.city,
            ZipCode: cust.address.zipcode,
            Latitude: cust.address.geo.lat,
            Longitude: cust.address.geo.lng
        }
 

        customers.push(customer)
        addresses.push(address);
    }

    return {customers: faker.helpers.shuffle(customers), addresses: faker.helpers.shuffle(addresses)}
}

function generateAddresses(customers) {
    let addresses = []

    for (let customer of customers) {
        let address = {
            Street: faker.address.streetAddress(true),
            City: faker.address.city(),
            State: faker.address.state(),
            ZipCode: faker.address.zipCode(),
            Country: faker.address.country(),
            CustomerID: customer.CustomerID
        }

        addresses.push(address);
    }

    return addresses
}


function generateProducts(n) {
    let products = []
    let inventory = []

    for (let i =0; i < n; i++) {
        let product = {
            ProductID: faker.random.uuid(),
            ProductName: faker.commerce.productName(),
            Price: +faker.commerce.price(),
            Department: faker.commerce.department(),
            Color: faker.commerce.color(),
            Material: faker.commerce.productMaterial()
        }

        let stock = {
            ProductID: faker.random.uuid(),
            ProductName: faker.commerce.productName(),
            Quantity: Math.ceil(100 + Math.random() * 100000),
            Department: product.Department
        }

        products.push(product);
        inventory.push(stock)
    }

    return {products: faker.helpers.shuffle(products),
            inventory: faker.helpers.shuffle(inventory)
        }
}


function generateOrders(CustomerDataSet, ProductDataSet, n) {
    let orders = []
    let invoices = []

    let orderStatuses = ['Delivered','Delivered', 'Cancelled', 'Delivered', 'Delivered','Delivered']

    let paymentModes = ['Bank','Bank', 'CreditCard', 'Cheque', 'CreditCard', 'CreditCard', 'Bank']


    for (let i = 0;i < n; i++) {
        let customerIndex = Math.floor(Math.random() * CustomerDataSet.customers.length)
        let customer = CustomerDataSet.customers[customerIndex];


        let invoiceNo = faker.random.uuid();
        let quantity = 0;
        let amount = 0;
        let invoiceDate =  faker.date.between(new Date(2012, 1, 1), new Date(2018, 1, 1), )
        let orderState = orderStatuses[0 + Math.floor(Math.random() * orderStatuses.length)];

        let paymentMode = paymentModes[0 + Math.floor(Math.random() * paymentModes.length)];

        for (let tid = 0; tid < (1 + Math.ceil(Math.random() * 5)); tid++) {
            let productIndex = Math.floor(Math.random() * ProductDataSet.products.length)
            let product = ProductDataSet.products[productIndex];

            let order = {
                InvoiceNo: invoiceNo,
                ItemPrice: +product.Price,
                Quantity: 1 + Math.floor(Math.random() * 10),
                ProductID: product.ProductID,
                CustomerID: customer.CustomerID,
                InvoiceDate: invoiceDate
            }

            quantity += order.Quantity
            amount += order.Quantity * order.ItemPrice

            orders.push(order);
        }

        let invoice = {
            InvoiceNo: invoiceNo,
            Amount: amount,
            Quantity: quantity,
            InvoiceDate: invoiceDate,
            Status: orderState,
            PaymentType: paymentMode,
            CustomerID: customer.CustomerID,
        }

        invoices.push(invoice)
        
    }

    return {orders: faker.helpers.shuffle(orders), 
            invoices:faker.helpers.shuffle(invoices)
        };
}


function writeToCsv(records,   filepath) {
        const record = records[0]
        console.log(Object.keys(record))
        let headers = []
        for (let k of Object.keys(record)) {
            let head = {
                id: k,
                title: k
            }
            headers.push(head)
        }

        console.log(headers)

        const csvWriter = createCsvWriter({
            path: filepath,
            header: headers
        });
         
        csvWriter.writeRecords(records)       // returns a promise
            .then(() => {
                console.log('...Done'. filepath);
            });

}


function saveFiles(directoryPath, CustomerDataSet, ProductDataSet, OrderDataSet) {
     
    var parentPath = path.join(__dirname, directoryPath)
    mkdirp.sync(parentPath)

    writeToCsv(CustomerDataSet.customers,
            path.join(parentPath,  "customers.csv"));

    
    writeToCsv( CustomerDataSet.addresses,
        path.join(parentPath,  "addresses.csv"));
    
    
    writeToCsv(ProductDataSet.products,
        path.join(parentPath,  "products.csv"));
    

    writeToCsv(ProductDataSet.inventory,
        path.join(parentPath,  "inventory.csv"));
    

    writeToCsv(OrderDataSet.orders,
        path.join(parentPath,  "orders.csv"));
    

    writeToCsv( OrderDataSet.invoices,
        path.join(parentPath,  "invoices.csv"));
    
    

    
    //     jsonfile.writeFile(path.join(parentPath,  "customers.json"), 
    //     CustomerDataSet.customers, 
    //                 { spaces: 2 }, function (err) {
    //         if (err) console.error(err)
    //         })

    // jsonfile.writeFile(path.join(parentPath,  "addresses.json"), 
    //                     CustomerDataSet.addresses, 
    //                     { spaces: 2 }, function (err) {
    //     if (err) console.error(err)
    // })


    // jsonfile.writeFile(path.join(parentPath,  "products.json"), 
    //                     ProductDataSet.products, 
    //                     { spaces: 2 }, function (err) {
    //     if (err) console.error(err)
    // })

    // jsonfile.writeFile(path.join(parentPath,  "inventory.json"), 
    //                     ProductDataSet.inventory, 
    //                     { spaces: 2 }, function (err) {
    //     if (err) console.error(err)
    // })

    // jsonfile.writeFile(path.join(parentPath,  "orders.json"), 
    //                     OrderDataSet.orders, 
    //                     { spaces: 2 }, function (err) {
    //     if (err) console.error(err)
    // })

    // jsonfile.writeFile(path.join(parentPath,  "invoices.json"), 
    //                     OrderDataSet.invoices, 
    //                     { spaces: 2 }, function (err) {
    //     if (err) console.error(err)
    // })
}


const CUSTOMERS = 100000
const PRODUCTS = 50000
const ORDERS = 200000


let CustomerDataSet = generateCustomers(CUSTOMERS)
console.log(CustomerDataSet)

let ProductDataSet = generateProducts(PRODUCTS) 
console.log(ProductDataSet)
 
console.log(faker.helpers.createTransaction())

let OrderDataSet = generateOrders(CustomerDataSet, ProductDataSet, ORDERS)

console.log(faker.date.between(new Date(2014, 1, 1), new Date(2018, 1, 1), ))


saveFiles("data/minimal", CustomerDataSet, ProductDataSet, OrderDataSet)
