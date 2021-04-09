import sys
import main

if __name__ == "__main__":  
    '''
    mode 0 -> non_urgent_mode
    mode 1 -> urgent mode
    '''        
    batch_mode = sys.argv[1] if len(sys.argv) >1 else 0
    main.main(batch_mode = batch_mode)
    
    