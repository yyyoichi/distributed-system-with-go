package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

const (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entwidth        = offWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	// 現在のサイズ
	idx.size = uint64(fi.Size())
	// maxにfileサイズを切り詰める（大きい場合余分に、小さい場合切り捨てる）
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}
	// fileとメモリを全て対応付ける
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		// 読込書込み許可
		gommap.PROT_READ|gommap.PROT_WRITE,
		// 他processからのアクセス許可
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		// 最後のデータ
		out = uint32((i.size / entwidth) - 1)
	} else {
		out = uint32(in)
	}
	// 対象のメモリ開始位置
	pos = uint64(out) * entwidth
	if i.size < pos+entwidth {
		return 0, 0, io.EOF
	}
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entwidth])
	return out, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error {
	if i.isMaxed() {
		return io.EOF
	}
	// 32bitのoffを末尾に追加
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	// 続いて64bitのposを追加
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entwidth], pos)
	// サイズ更新
	i.size += uint64(entwidth)
	return nil
}

// 最大サイズに達しているか（次に32bi+64bit追加した場合）
func (i *index) isMaxed() bool {
	return uint64(len(i.mmap)) < i.size+entwidth
}

func (i *index) Name() string {
	return i.file.Name()
}
