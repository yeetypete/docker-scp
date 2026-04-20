package main

import (
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/containerd/containerd/v2/core/images"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

// layerBar renders one row per layer: Pushing → Pushed → Extracting → Push
// complete. The bar is built with total=0 so mpb's triggerComplete stays off,
// letting current reach size without auto-completing. We complete explicitly
// via SetTotal(size, true) once extract finishes.
type layerBar struct {
	bar  *mpb.Bar
	size int64

	transferDone atomic.Bool
	extractStart atomic.Int64
	extractEnd   atomic.Bool
}

// Methods on *layerBar are safe to call on a nil receiver so callers holding a
// bar handle don't need to null-check (non-layer descriptors have no bar).

// transferFinish advances the bar to full and flips to the Pushed label.
// SetCurrent is idempotent when the streamed proxyReader already reached size.
func (lb *layerBar) transferFinish() {
	if lb == nil {
		return
	}
	lb.bar.SetCurrent(lb.size)
	lb.transferDone.Store(true)
}

func (lb *layerBar) proxyReader(r io.Reader) io.Reader {
	if lb == nil {
		return r
	}
	return lb.bar.ProxyReader(r)
}

func (lb *layerBar) extractBegin() {
	if lb == nil {
		return
	}
	lb.extractStart.Store(time.Now().UnixNano())
}

func (lb *layerBar) extractFinish() {
	if lb == nil {
		return
	}
	lb.extractEnd.Store(true)
	lb.bar.SetTotal(lb.size, true)
}

func (lb *layerBar) abort() {
	if lb == nil {
		return
	}
	lb.bar.Abort(true)
}

type progressState struct {
	p    *mpb.Progress
	mu   sync.Mutex
	bars map[digest.Digest]*layerBar
}

func newProgressState(p *mpb.Progress) *progressState {
	return &progressState{p: p, bars: make(map[digest.Digest]*layerBar)}
}

// layerFor returns a bar for desc if it is a layer, nil otherwise. Non-layer
// blobs (manifests, config, index) transfer silently.
func (s *progressState) layerFor(desc ocispec.Descriptor) *layerBar {
	if !images.IsLayerType(desc.MediaType) {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if b, ok := s.bars[desc.Digest]; ok {
		return b
	}
	lb := s.newBar(desc)
	s.bars[desc.Digest] = lb
	return lb
}

func (s *progressState) lookup(d digest.Digest) *layerBar {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bars[d]
}

// finalize closes out any bars whose extract phase never ran (layer was
// already unpacked on the remote). Without this, prog.Wait blocks forever on
// bars with triggerComplete still unset.
func (s *progressState) finalize() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, lb := range s.bars {
		if !lb.extractEnd.Load() {
			lb.extractEnd.Store(true)
			lb.bar.SetTotal(lb.size, true)
		}
	}
}

func (s *progressState) newBar(desc ocispec.Descriptor) *layerBar {
	lb := &layerBar{size: desc.Size}
	id := short(desc.Digest)

	label := decor.Any(func(_ decor.Statistics) string {
		switch {
		case lb.extractEnd.Load():
			return id + ": Push complete"
		case lb.extractStart.Load() != 0:
			return id + ": Extracting"
		case lb.transferDone.Load():
			return id + ": Pushed"
		default:
			return id + ": Pushing"
		}
	})

	counters := decor.CountersKiloByte(" %.2f/%.2f")
	conditionalCounters := decor.Any(func(st decor.Statistics) string {
		if lb.extractStart.Load() != 0 || lb.extractEnd.Load() {
			return ""
		}
		s, _ := counters.Decor(st)
		return s
	})

	lb.bar = s.p.New(0,
		newDynamicFiller(lb),
		mpb.PrependDecorators(label),
		mpb.AppendDecorators(conditionalCounters),
	)
	lb.bar.SetTotal(desc.Size, false)
	return lb
}

var spinnerFrames = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

// dynamicFiller is the bar's renderer: a progress bar during transfer, spinner
// + timer during extract, nothing once finalized. Serves as its own
// BarFillerBuilder via Build.
type dynamicFiller struct {
	lb    *layerBar
	bar   mpb.BarFiller
	count uint
}

func newDynamicFiller(lb *layerBar) *dynamicFiller {
	return &dynamicFiller{
		lb:  lb,
		bar: mpb.BarStyle().Padding(" ").Build(),
	}
}

func (d *dynamicFiller) Build() mpb.BarFiller { return d }

func (d *dynamicFiller) Fill(w io.Writer, st decor.Statistics) error {
	switch {
	case d.lb.extractEnd.Load():
		return nil
	case d.lb.extractStart.Load() != 0:
		frame := spinnerFrames[d.count%uint(len(spinnerFrames))]
		d.count++
		dur := time.Since(time.Unix(0, d.lb.extractStart.Load()))
		_, err := io.WriteString(w, frame+" "+dur.Round(time.Second).String())
		return err
	default:
		return d.bar.Fill(w, st)
	}
}
