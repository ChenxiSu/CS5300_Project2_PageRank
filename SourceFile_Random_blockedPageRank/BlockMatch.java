public class BlockMatch {
    private static long[] b = {
      10327, 20372, 30628, 40644, 50461, 60840, 70590, 80117, 90496, 
      100500, 110566, 120944, 130998, 140573, 150952, 161331, 171153, 
      181513, 191624, 202003, 212382, 222761, 232592, 242877, 252937, 
      263148, 273209, 283472, 293254, 303042, 313369, 323521, 333882, 
      343662, 353644, 363928, 374235, 384553, 394928, 404711, 414616,
      424746, 434706, 444488, 454284, 464397, 474195, 484049, 493967, 
      503751, 514130, 524509, 534708, 545087, 555466, 565845, 576224, 
      586603, 596584, 606366, 616147, 626447, 636239, 646021, 655803, 
      665665, 675447, 685229
    };
	
   //  public static long blockIDofNode(long n) {
  	// 	int h= -1; 
  	// 	int t= b.length;
  	// 	while (h != t - 1) {		
  	// 	    int e = ( h + t ) / 2;
  	// 	    if (n <= b[e]) t= e;
  	// 	    else h= e;
  	// 	}
  	// 	return t;
	  // }	
    public static long blockIDofNode(long n) {
      return n % (b.length);
    }

    /*
      return the smallest Node ID of a specified block
    */
    public static long getSmallestNodeIdByBlockId(long curBlockId){

        if(curBlockId == 0) return 0; 
        else{
          long lastNodeLargestId = b[(int)curBlockId-1];
          return lastNodeLargestId+1 ;
        }
    }
}
