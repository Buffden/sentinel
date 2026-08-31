import TopNav from '@/widgets/top-nav/TopNav'
import Footer from '@/shell/Footer'
import SplitLayout from '@/shell/SplitLayout'
import MapWidget from '@/widgets/map-widget/MapWidget'
import Workspace from '@/workspace/Workspace'

export default function Page() {
	return (
		<div style={{ display: 'flex', flexDirection: 'column', height: '100vh', width: '100vw', overflow: 'hidden' }}>
			<TopNav />

			<SplitLayout
				left={<MapWidget />}
				right={<Workspace />}
			/>

			<Footer />
		</div>
	)
}
