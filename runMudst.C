/*
  Orig Author: Akio Ogawa
  Edited: David Kapukchyan
  Edited2: Sean Preins
  @[May 18, 2022](David Kapukchyan)
  > Copied from $STAR/StRoot/StFcsFastSimulatorMaker/macros/runMudst.C and modified so that I can test my own StFcsWaveformFitMaker.
  @[Sept 27, 2023](Sean Preins)
  > Got file from David, deleted code related to waveform fit maker analysis, from Fcs2019/FcsAna22   
  Macro to run micro dst from fast production of FCS data for Run 22 data.
*/

void runMudst(const char* file="/star/data101/TEMP/FCS/st_fwd_23074018_raw_4500027.MuDst.root",
	      int ifile = -1,
              int nevt = 10,
              int readMuDst = 1
	      ){

  gROOT->Macro("Load.C");
  gROOT->Macro("$STAR/StRoot/StMuDSTMaker/COMMON/macros/loadSharedLibraries.C");
  gSystem->Load("StEventMaker");
  gSystem->Load("StFcsDbMaker");
  gSystem->Load("StFcsRawHitMaker");
  gSystem->Load("StFcsWaveformFitMaker");
  gSystem->Load("StFcsClusterMaker");
  gSystem->Load("libMinuit");
  gSystem->Load("StFcsPointMaker");
  gSystem->Load("StSpinDbMaker");

  gMessMgr->SetLimit("I", 0);
  gMessMgr->SetLimit("Q", 0);
  gMessMgr->SetLimit("W", 0);

  gStyle->SetOptDate(0);
  
  StChain* chain = new StChain("StChain"); chain->SetDEBUG(0);
  StMuDstMaker* muDstMaker = new StMuDstMaker(0, 0, "", file,".", 1000, "MuDst");
  int n=muDstMaker->tree()->GetEntries();
  printf("Found %d entries in Mudst\n",n);
  
    int start=0, stop=n;
    if(ifile>=0){
    int start=ifile*nevt;
    int stop=(ifile+1)*nevt-1;
    if(n<start) {printf(" No event left. Exiting\n"); return;}
    if(n<stop)  {printf(" Overwriting end event# stop=%d\n",n); stop=n;}
    }else if(nevt>=0 && nevt<n){
    stop=nevt;
    }else if(nevt==-2){
    stop=2000000000; 
    }
    printf("Doing Event=%d to %d\n",start,stop);
  
  
  St_db_Maker* dbMk = new St_db_Maker("db","MySQL:StarDb","$STAR/StarDb"); 
  if(dbMk){
    dbMk->SetAttr("blacklist", "tpc");
    dbMk->SetAttr("blacklist", "svt");
    dbMk->SetAttr("blacklist", "ssd");
    dbMk->SetAttr("blacklist", "ist");
    dbMk->SetAttr("blacklist", "pxl");
    dbMk->SetAttr("blacklist", "pp2pp");
    dbMk->SetAttr("blacklist", "ftpc");
    dbMk->SetAttr("blacklist", "emc");
    dbMk->SetAttr("blacklist", "eemc");
    dbMk->SetAttr("blacklist", "mtd");
    dbMk->SetAttr("blacklist", "pmd");
    dbMk->SetAttr("blacklist", "tof");
    dbMk->SetAttr("blacklist", "etof");
    dbMk->SetAttr("blacklist", "rhicf");
  }
  cout<<"Init DB Maker"<<endl;  

  StSpinDbMaker *spinDbMker = new StSpinDbMaker("spinDb");
  cout<<"Get SpinDbMaker"<<endl;
  StFcsDbMaker *fcsDbMkr= new StFcsDbMaker();
  cout<<"Get FCSDB"<<endl;
  StFcsDb* fcsDb = (StFcsDb*) chain->GetDataSet("fcsDb");
  cout<<"Get FCS Db pulse"<<endl;
  StFcsDbPulse* fcsDbPulse = (StFcsDbPulse*) chain->GetDataSet("fcsPulse");
  cout<<"Get event maker"<<endl;
  StEventMaker* eventMk = new StEventMaker();
  cout<<"Get hit maker"<<endl;
  StFcsRawHitMaker* hit = new StFcsRawHitMaker();
  cout<<"Finish FCS"<<endl;
  //hit->setDebug();
  hit->setReadMuDst(readMuDst);
  
  StFcsWaveformFitMaker *wff= new StFcsWaveformFitMaker();
  wff->setEnergySelect(13,13,1);  

  gSystem->Load("StEpdUtil");
  gSystem->Load("SimpleTree");
  SimpleTree* simptr = new SimpleTree();
  TString foriternum(file);
  Ssiz_t last_ = foriternum.Last('_');
  TString iternum = foriternum(last_+1,7);
  TString outName = "SimpleTree_"+iternum+".root";
  simptr->SetOutputFileName(outName.Data());
  chain->Init();
  cout<<"After init"<<endl;
  //chain->EventLoop(start,stop);
  //chain->EventLoop(5);



  TStopwatch clock;
  //Event loop
  for( UInt_t i=0; i<nevt; ++i ){
    chain->Make();
    chain->Clear();
  }
  std::cout << "========================================" << std::endl;
  std::cout << clock.RealTime() << " seconds" << std::endl;
  std::cout << "========================================" << std::endl;

  chain->Finish();
  delete chain;
 
}

