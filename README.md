# Taichi
A big data analysis engine based on bitmap, targeting very little resource (disk, cpu, memory) and fast calculating, and long term storage.

Generally, big data analysis for internet business require UV(number of identical individuals), PV(total visits), retention(the rate of the individaul remain compared) analysis etc. The common solution is to use database/data storage which is heavy and costy.
A quick and lightweight solution is to use bitmap to do such analysis and store history data. The Taichi project is targetting such a solution that:
1.	PC level resource (like 8-core cpu and 16G memory, with 1T hard disk storage) can hold ten millions of data per business lines, and ten thousands of business lines; 
2.	and all these data may store many years without disk extension
3.	all data and analysis result are 100% accurate
4.	Fast – single operation may cost less than 1ms; a calcation of thousand business lines may cost less than 5s; support non-blocking operations for all mess calculation.
5.	Support group operation and complex formula
6.	Support HA

As a design result, Taichi may NOT support individual detail other than UV, like you may search if one client do visit on specific date in a specific action, but cannot store/find how long does he spend on that action; 
Bitmap is based on 32-bit integer, though Taichi does support bigger ids. However it will cost much more if the id is over N*232 , which N is > 16. An alternative solution in Taichi is keep a mapping between the real id and stored id, which total count of stored-id is smaller than N*232 . For example, mobile device is usually 64-bit long, but actually the existing and potential customers may be much smaller than 232 .

Again, Taichi is targetting small and fast data analyis for UV, PV, retention and other operations based on that. Developer may need common calution if other analysis required like dividual detail.
