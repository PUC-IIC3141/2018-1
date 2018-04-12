package simpledb.index.bloomfilter;

import simpledb.file.Block;
import simpledb.tx.Transaction;

public class BloomFilterPage {
	
	private Block blk;
	private Transaction tx;

	
	public BloomFilterPage(Block blk, Transaction tx) {
		this.blk = blk;
		this.tx = tx;
		tx.pin(blk);
	}
	
	
	public boolean getBit(int pos) {
		byte currentByte = tx.getByte(blk, pos/8);
		int bitpos = pos%8;
		return (currentByte >>> (7-bitpos)) % 2 == 1;
	}

	
	public void setBit(int pos, boolean value) {
		byte b = tx.getByte(blk, pos/8);
		int bitpos = pos%8;
		int mask = 1 << (7 - bitpos);

		if (value) {
			b = (byte) ( (b | mask) & 0xFF);
		}
		else {
			mask = (~mask) & 0xFF;
			b = (byte) ( (b & mask) & 0xFF);
		}

		tx.setByte(blk, pos/8, b);
	}

	
	public void close() {
		if (blk != null) {
			tx.unpin(blk);
			blk = null;
		}
	}
}
