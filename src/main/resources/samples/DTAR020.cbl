00100*                                                                         
00200*   DTAR020 IS THE OUTPUT FROM DTAB020 FROM THE IML                       
00300*   CENTRAL REPORTING SYSTEM                                              
00400*                                                                         
00500*   CREATED BY BRUCE ARTHUR  19/12/90                                     
00600*                                                                         
00700*   RECORD LENGTH IS 27.                                                  
00800*                                                                         
00900        03  DTAR020-KCODE-STORE-KEY.                                      
01000            05 DTAR020-KEYCODE-NO      PIC X(08).                         
01100            05 DTAR020-STORE-NO        PIC S9(03)   COMP-3.               
01200        03  DTAR020-DATE               PIC S9(07)   COMP-3.               
01300        03  DTAR020-DEPT-NO            PIC S9(03)   COMP-3.               
01400        03  DTAR020-QTY-SOLD           PIC S9(9)    COMP-3.               
01500        03  DTAR020-SALE-PRICE         PIC S9(9)V99 COMP-3.               

