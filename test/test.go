package test

import (
	"net/http"
	"strings"
	"time"

	"github.com/ozontech/file.d/plugin/output/elasticsearch"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/input/fake"
	"github.com/ozontech/file.d/plugin/output/devnull"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type Opts []string

func (o Opts) Has(opt string) bool {
	return strings.Contains(strings.Join(o, "-"), opt)
}

type Case struct {
	Prepare func()
	Act     func(pipeline *pipeline.Pipeline)
	Assert  func(pipeline *pipeline.Pipeline)
	Out     func(event *pipeline.Event)
}

func RunCase(testCase *Case, inputInfo *pipeline.InputPluginInfo, eventCount int, pipelineOpts ...string) {
	testCase.Prepare()

	p := startCasePipeline(testCase.Act, testCase.Out, eventCount, inputInfo, pipelineOpts...)

	testCase.Assert(p)
}

func startCasePipeline(act func(pipeline *pipeline.Pipeline), out func(event *pipeline.Event), eventCount int, inputInfo *pipeline.InputPluginInfo, pipelineOpts ...string) *pipeline.Pipeline {
	x := atomic.NewInt32(int32(eventCount))

	pipelineOpts = append(pipelineOpts, "passive")
	p := NewPipeline(nil, pipelineOpts...)

	p.SetInput(inputInfo)

	anyPlugin, config := devnull.Factory()
	outputPlugin := anyPlugin.(*devnull.Plugin)
	p.SetOutput(&pipeline.OutputPluginInfo{
		PluginStaticInfo: &pipeline.PluginStaticInfo{
			Config: config,
		},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: outputPlugin,
		},
	})

	outputPlugin.SetOutFn(func(event *pipeline.Event) {
		x.Dec()
		if out != nil {
			out(event)
		}
	})
	p.Start()

	act(p)

	t := time.Now()
	for {
		time.Sleep(time.Millisecond * 10)
		if x.Load() <= 0 {
			break
		}
		if time.Since(t) > time.Second*10 {
			panic("too long act")
		}
	}
	p.Stop()

	return p
}

func WaitForEvents(x *atomic.Int32) {
	t := time.Now()
	for {
		time.Sleep(time.Millisecond * 10)
		if x.Load() <= 0 {
			break
		}
		if time.Since(t) > time.Second*10 {
			panic("too long wait")
		}
	}
}

func NewPipeline(actions []*pipeline.ActionPluginStaticInfo, pipelineOpts ...string) *pipeline.Pipeline {
	parallel := Opts(pipelineOpts).Has("parallel")
	perf := Opts(pipelineOpts).Has("perf")
	mock := Opts(pipelineOpts).Has("mock")
	passive := Opts(pipelineOpts).Has("passive")
	elastic := Opts(pipelineOpts).Has("elastic")

	eventTimeout := pipeline.DefaultEventTimeout
	if Opts(pipelineOpts).Has("short_event_timeout") {
		eventTimeout = 10 * time.Millisecond
	}

	if perf {
		parallel = true
	}

	settings := &pipeline.Settings{
		Capacity:            1024,
		MaintenanceInterval: time.Second * 5,
		EventTimeout:        eventTimeout,
		AntispamThreshold:   0,
		AvgEventSize:        16 * 1024,
		StreamField:         "stream",
		Decoder:             "json",
	}

	http.DefaultServeMux = &http.ServeMux{}
	p := pipeline.New("test_pipeline", settings, prometheus.NewRegistry())
	if !parallel {
		p.DisableParallelism()
	}

	if !perf {
		p.EnableEventLog()
	}

	if mock {
		anyPlugin, _ := fake.Factory()
		inputPlugin := anyPlugin.(*fake.Plugin)
		p.SetInput(&pipeline.InputPluginInfo{
			PluginStaticInfo: &pipeline.PluginStaticInfo{
				Type: "fake",
			},
			PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
				Plugin: inputPlugin,
			},
		})

		if elastic {
			anyPlugin, anyConfig := elasticsearch.Factory()
			outputPlugin := anyPlugin.(*elasticsearch.Plugin)
			elasticConfig := anyConfig.(*elasticsearch.Config)
			elasticConfig.Endpoints = []string{"http://host.docker.internal:9200"}
			elasticConfig.BatchSize = "256"
			err := cfg.Parse(elasticConfig, map[string]int{"gomaxprocs": 1})
			if err != nil {
				panic(err.Error())
			}
			p.SetOutput(&pipeline.OutputPluginInfo{
				PluginStaticInfo: &pipeline.PluginStaticInfo{
					Type:   "elasticsearch",
					Config: anyConfig,
				},
				PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
					Plugin: outputPlugin,
				},
			})
		} else {
			anyPlugin, _ = devnull.Factory()
			outputPlugin := anyPlugin.(*devnull.Plugin)
			p.SetOutput(&pipeline.OutputPluginInfo{
				PluginStaticInfo: &pipeline.PluginStaticInfo{
					Type: "devnull",
				},
				PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
					Plugin: outputPlugin,
				},
			})
		}
	}

	for _, info := range actions {
		p.AddAction(info)
	}

	if !passive {
		p.Start()
	}

	return p
}

func NewPipelineMock(actions []*pipeline.ActionPluginStaticInfo, pipelineOpts ...string) (*pipeline.Pipeline, *fake.Plugin, *devnull.Plugin) {
	pipelineOpts = append(pipelineOpts, "mock")
	p := NewPipeline(actions, pipelineOpts...)

	return p, p.GetInput().(*fake.Plugin), p.GetOutput().(*devnull.Plugin)
}

func NewPipelineMock2(actions []*pipeline.ActionPluginStaticInfo, pipelineOpts ...string) (*pipeline.Pipeline, *elasticsearch.Plugin) {
	pipelineOpts = append(pipelineOpts, "mock", "elastic")
	p := NewPipeline(actions, pipelineOpts...)

	return p, p.GetOutput().(*elasticsearch.Plugin)
}

func NewPluginStaticInfo(factory pipeline.PluginFactory, config pipeline.AnyConfig) *pipeline.PluginStaticInfo {
	return &pipeline.PluginStaticInfo{
		Type:    "test_plugin",
		Factory: factory,
		Config:  config,
	}
}

func NewActionPluginStaticInfo(factory pipeline.PluginFactory, config pipeline.AnyConfig, mode pipeline.MatchMode, conds pipeline.MatchConditions, matchInvert bool) []*pipeline.ActionPluginStaticInfo {
	return []*pipeline.ActionPluginStaticInfo{
		{
			PluginStaticInfo: NewPluginStaticInfo(factory, config),
			MatchConditions:  conds,
			MatchMode:        mode,
			MatchInvert:      matchInvert,
		},
	}
}

func NewEmptyOutputPluginParams() *pipeline.OutputPluginParams {
	return &pipeline.OutputPluginParams{
		PluginDefaultParams: &pipeline.PluginDefaultParams{
			PipelineName:     "test_pipeline",
			PipelineSettings: &pipeline.Settings{},
		},
		Controller: nil,
		Logger:     zap.L().Sugar(),
	}
}

func NewConfig(config interface{}, params map[string]int) interface{} {
	err := cfg.Parse(config, params)
	if err != nil {
		logger.Panicf("wrong config: %s", err.Error())
	}

	return config
}
